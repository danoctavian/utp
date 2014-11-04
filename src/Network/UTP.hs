{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveDataTypeable #-} 

module Network.UTP where

import Network.Socket as NS
import Network.Socket.ByteString as NSB
import Data.Word
import Data.Serialize as DS
import Control.Applicative
import Control.Monad
import Prelude as P
import Data.ByteString.Char8 as DBC
import Data.Bits
import Data.Maybe
import Data.Tuple
import Control.Concurrent.STM
import Data.ByteString as BS
import System.Random
import Data.Dequeue as DQ
import Control.Monad.IO.Class
import Data.Time.Clock.POSIX
import Control.Concurrent
import System.Log.Logger
import System.Log.Handler.Syslog
import Control.Concurrent.STM.TChan
import Control.Concurrent
import Control.Monad.Trans.Either
import Data.Either.Combinators
import Control.Concurrent.Async
import Control.Exception
import Data.Typeable
import Data.HashTable.IO
import Data.Map.Strict as Map 
-- interface?
{-

what i need: 
map udp connection to utp connection

upgradeToUTP :: Socket -> IO Socket

functions:
* send
* recv
* recvLen

ignore listen/accept and all the other stuff for now for now

Packet based or stream based? The way the interface is now
it's stream based.

incoming-buffer
|    ||         ||      ||
just append at the end i guess? append is O(N + m)
how to consume ? split bytestring
-- buffer size is fixed; potentially changed by the formula

CIRCULAR BUFFER IDEA
a circular buffer can achieve the adding and consuming of the buffer
no problem about providing a stream based interface here.

need to work with some sort of mutable bytestrings.

buffer expansion means:  copy everything in the new buffer
  if bigger all good
  if smaller ? do you just dump packets

dump the circular packet idea. it's problematic if an ack for a packet inbetween other packets comes in. how do you deal with the whole without copying a large amount of data?

LEAVE OUT SELECTIVE ACK for now

need to send continous keepalive (ack for the current packet)

keep acking regularly for the last received package. if you receive 

there's a haskell package for UTP by sam truszan
but that code is just too smart for me

EXCEPTION HANDLING AND CLOSING

exceptions:
udp connection is dropped
  happens in recv thread or send thread. set conn state to ERR
  next time a user calls send or recv he gets the error
   < quite complicated > 
timeout?
  leave it to the connection user to handle; block indefinetly if needed
malformed packets coming our way -> just ignore them

CLOSURE

need to kill associated threads
  recv thread
  ack thread
  resend thread
keep track of their threadId in the ConnData
close socket as well


-}

{-

TODO: missing features

** window resizing
** rtt based timeouts 
-}


-- logging
utplogger = "utplog"

-- packet parsing

type SeqNum = Word16
type AckNum = Word16
type Time = Word32
type ConnectionId = Word16

data PacketType = ST_DATA | ST_FIN | ST_STATE | ST_RESET | ST_SYN 
  deriving (Eq, Show)
data Packet = Packet {
    packetType :: PacketType,
    version :: Word8,
    extensions :: Word8, -- ignore it for now
    connectionId :: Word16,
    time :: Time,
    timeDiff :: Time,
    windowSize :: Word32,
    seqNum :: SeqNum,
    ackNum :: AckNum,
    payload :: ByteString
  } deriving (Show, Eq)

-- CONSTANTS
headerSize = BS.length $ DS.encode $ Packet ST_DATA 0 0 0 0 0 0 0 0 ""
defWindowSize = 500
recvBufferSize = 10000
recvSize = 2048 -- TODO: really need to have some logic behind this value

defResendTimeout = 5 * 10 ^ 5

packetSize p = headerSize + (BS.length $ payload p)

-- the connection returned
data Connection = Conn {
    send :: ByteString -> IO ()
  , recvLen :: Int -> IO (ByteString, Int)
  , recv :: Int -> IO ByteString
  , close :: IO ()
  }


data ConnStage = CS_SYN_SENT | CS_CONNECTED | ERROR deriving (Show, Eq)

data ConnData = ConnData {
    connState :: TVar ConnState
  , inBuf :: TVar (BankersDequeue ByteString) 
  , outBuf :: TVar (BankersDequeue Packet)
  , connSocket :: Socket
  , connIdRecv ::ConnectionId
  , connIdSend ::ConnectionId
  }


-- dumb summing up of sizes
-- optimize by keeping track of size on removal and insertion
dqSize :: Dequeue q => q a -> (a -> Int)  -> Int
dqSize dq len = P.sum $ P.map len $ dqToList dq

dqToList dq = DQ.takeFront (DQ.length dq) dq


data ConnState = ConnState {
    connSeqNum :: SeqNum
  , connAckNum :: AckNum
  , maxWindow :: Word32
  , peerMaxWindow :: Word32 -- window size advertised by peer
  , replyMicro :: Time
  , connStage :: ConnStage
  }

packetTypeMap = P.zip [0, 1..] [ST_DATA, ST_FIN, ST_STATE, ST_RESET, ST_SYN]

getTypeVersion = do
  byte <- getWord8 
  let packType = P.lookup (shiftR byte 4) packetTypeMap
  let version = shiftL (shiftL byte 4) 4
  case packType of 
    Just typeVal ->  return (typeVal, version)
    Nothing -> fail "unknown packet type"


getRest = remaining >>= getBytes
instance Serialize Packet where
  get = (\(t, v) -> Packet t v) <$> getTypeVersion <*> getWord8 <*> getWord16be
                                <*>  getWord32be <*> getWord32be <*> getWord32be
                                <*> getWord16be  <*> getWord16be <*> getRest
  -- assumes valid packet
  put Packet {..} = do
    putWord8 ((shiftR (fromJust $ P.lookup packetType $
                P.map swap packetTypeMap) 4) + version)
    putWord8 extensions 
    putWord16be connectionId
    putWord32be timeDiff 
    putWord32be windowSize
    putWord16be seqNum 
    putWord16be ackNum 
    putByteString payload


-- warning: partial initialization
makePacket pType load conn
  = Packet {packetType = pType, payload = load, connectionId = connIdRecv conn}
{-

  this function sets the following fields of packet
    time :: Time
    timeDiff :: Time
    windowSize :: Word32
    seqNum :: SeqNum
    ackNum :: AckNum
-} 
sendPacket packet conn = do
  sequenced <- atomically $ do
    state <- readTVar $ connState conn 
    out <- readTVar $ outBuf conn
    -- if there is no space in the buffer just block
    when (maxWindow state < fromIntegral ((packetSize packet) + dqSize out packetSize))
      retry
    writeTVar (outBuf conn) (pushBack out packet)
    let currSeq = connSeqNum state
    writeTVar (connState conn) (state {connSeqNum = currSeq + 1})

    inB <- readTVar $ inBuf conn
    return $ packet {seqNum = currSeq, ackNum = connAckNum state
                    , timeDiff = replyMicro state
                    , windowSize = fromIntegral $ dqSize inB (BS.length)}
  micros <- getTimeMicros 
  liftIO $ NSB.send (connSocket conn) (DS.encode $ sequenced {time = micros})        
  return ()

-- TODO: remove
fooConn = Conn (\bs -> return ()) (\n -> return ("", 0)) (\n -> return "") (return () )


-- what a client calls
utpConnect :: MonadIO m => Socket -> m Connection
utpConnect sock = do
  g <- liftIO $ newStdGen
  let randId = P.head $ (randoms g :: [Word16])
  let initState = ConnState {connSeqNum = 1,
                            connAckNum = 0,
                            connStage = CS_SYN_SENT,
                            maxWindow = defWindowSize,
                            peerMaxWindow = defWindowSize,
                            replyMicro = 0}
 
  stateVar <- liftIO $ newTVarIO initState 
  inBufVar <- liftIO $ newTVarIO DQ.empty 
  outBufVar <- liftIO $ newTVarIO DQ.empty 
  let conn = ConnData stateVar inBufVar outBufVar sock randId (randId + 1)

  startHS <- liftIO $ getTimeMicros
  liftIO $ sendPacket (makePacket ST_SYN "" conn) conn

  -- run resend thread
  liftIO $ forkIO $ setInterval defResendTimeout $ resendOutgoing conn

  asyncRecv <- liftIO $ async (ackRecv conn) -- waiting for the first ack
  fstAck <- liftIO $ wait asyncRecv
  endHS <- liftIO $ getTimeMicros 
  let initRtt = endHS - startHS 
   
  -- things worked out 
  liftIO $ atomically $ modifyTVar (connState conn)
           (\s -> s {connStage = CS_CONNECTED, connAckNum = seqNum fstAck})


  -- block here until syn-ack stage is done
  return $ fooConn 
data UTPException = FailedHandshake deriving (Show, Typeable)
instance Exception UTPException

{- server call
  not optimized to handle a large number of calls
  good enough to handle p2p apps with something 
  like at most 100 connections

  TODO: figure out how to graciously kill this.
-}


utpListen :: Socket -> (SockAddr -> Connection -> IO()) -> IO ()
utpListen sock handle = do
  connMapVar <- newTVarIO Map.empty
  forever $ do
    (packet, sockAddr) <- recvPacket sock recvSize
    connMap <- atomically $ readTVar connMapVar
    case (Map.lookup sockAddr connMap) of
      Just inChan -> atomically $ writeTChan inChan packet
      Nothing -> do
        -- new connection - fork thread to handle this
        forkIO $ do
          inChan <- atomically $ do
            inChan <- newTChan
            modifyTVar connMapVar (Map.insert sockAddr inChan)
            return inChan
          (serverHandshake inChan sock sockAddr >>= handle sockAddr)
          `finally`
          (atomically $ modifyTVar connMapVar (Map.delete sockAddr) )
        return ()

serverHandshake :: TChan Packet -> Socket -> SockAddr -> IO Connection
serverHandshake packChan sock sockAddr  = do
  packet <- atomically $ readTChan packChan 
  when (packetType packet /= ST_SYN) $ throwIO FailedHandshake
  -- start acking and return the connection 
  return undefined

-- loops until it reads a valid packet
recvPacket sock recvSize = fmap unwrapLeft $ runEitherT $ forever $ do
    (msg, src) <- liftIO $ NSB.recvFrom sock recvSize
    case (DS.decode msg :: Either String Packet) of
      Left err -> do
        -- keep looping listening for packets
        -- TODO: maybe this should be more strict and close the connection
        liftIO $ errorM utplogger "Unparsable package"
      Right packet -> left (packet, src) -- exit loop

 
getTimeMicros = fmap (\n -> P.round $ n * 10 ^ 6) $ liftIO $ getPOSIXTime
setInterval t f = forever $ threadDelay t >> f

ackRecv conn = do
  packet <- fmap P.fst $ recvPacket (connSocket conn) recvSize
  case (packetType packet) of
    ST_STATE -> handleAck conn packet >> return packet
    _ -> do
      liftIO $ errorM utplogger "got something other than ack"
      liftIO $ throwIO FailedHandshake

resendSyn conn = do
  x <- runEitherT $ forever $ do
    state <- liftIO $ atomically $ readTVar (connState conn)    
    when (connStage state /= CS_SYN_SENT) $ left () -- exit the loop
    liftIO $ sendPacket (makePacket ST_SYN "" conn) conn
    liftIO $ threadDelay $ defResendTimeout
  return ()


-- takes first elems returning remaining DQ
dqTakeWhile :: Dequeue q => (a -> Bool) -> q a -> ([a], q a)
dqTakeWhile cond dq = case DQ.length dq of
  0 -> ([], dq)
  _ -> let (taken, rest) = dqTakeWhile cond (P.snd $ popFront dq) in 
         if' (cond $ fromJust $ DQ.first dq)
          ((fromJust $ DQ.first dq) : taken, rest) 
          ([], dq)


handleAck conn packet = atomically $ modifyTVar (outBuf conn)
                (P.snd . (dqTakeWhile ((<= ackNum packet) . seqNum)))

resendOutgoing conn = do
  outgoing <- atomically $ readTVar (outBuf conn)
  forM (dqToList outgoing) $ \p -> NSB.send (connSocket conn) $ DS.encode p
  return ()
 
 

recvIncoming :: ConnData -> Packet ->  IO ()
recvIncoming conn packet = case packetType packet of
  ST_SYN -> do
    atomically $ modifyTVar (connState conn)
                  (\s -> s {connAckNum = seqNum packet
                          , connStage = CS_CONNECTED})
    sendPacket (makePacket ST_STATE "" conn) conn
  ST_STATE -> handleAck conn packet
  ST_DATA -> do
    atomically $ do
      stateNow <- readTVar $ connState conn
      when (connAckNum stateNow + 1 == ackNum packet) $ do
        modifyTVar (inBuf conn) (P.flip DQ.pushBack $ payload packet)
        modifyTVar (connState conn) (\s -> s {connAckNum = seqNum packet})
    sendPacket (makePacket ST_STATE "" conn) conn -- ack

      
      

-- helpers 
if' c a b = if c then a else b
unwrapLeft (Left x) = x
