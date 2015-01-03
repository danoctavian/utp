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

{-

TODO: missing features

** refactor interface code to be like
  makeConn :: Settings -> ConnHandler -> IO ()
  instead of returning a connection. this way you execute the handler on your 
  own terms and are able to do resource cleanup on exceptions or when it's done
  therefore, stop using forkIO and start using async  for worker threads
** fix the resend policy; now you resend every 500ms regardless of when is the last
  time you received smth from the other side
** packet loss
** window resizing
** rtt based timeouts 
** keep alive
** circular buffer implementation for efficient send recv

** testing with unreliable networks
-}

{-
CIRCULAR BUFFER IDEA
a circular buffer can achieve the adding and consuming of the buffer
no problem about providing a stream based interface here.

need to work with some sort of mutable bytestrings.

buffer expansion/reduction means:  copy everything in the new buffer
  if bigger all good
  if smaller ? do you just dump packets


Acking ahead

maybe it's problematic if an ack for a packet inbetween other packets comes in. how do you deal with the whole without copying a large amount of data?

without selective ack - the above is a non issue 
with selective ack - you still need to wait for all packets to provide ordered data receive.so it's not reall an issue - 


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

-- logging
utplogger = "utplog"

-- packet parsing

type SeqNum = Word16
type AckNum = Word16
type Time = Word32
type ConnectionId = Word16
type SockSend = ByteString -> IO Int

data PacketType = ST_DATA | ST_FIN | ST_STATE | ST_RESET | ST_SYN 
  deriving (Eq, Show)

packetTypeMap = P.zip [0, 1..] [ST_DATA, ST_FIN, ST_STATE, ST_RESET, ST_SYN]

data Packet = Packet {
    packetType :: PacketType,
    version :: Word8,
    extensions :: Word8, -- ignore it for now
    connId :: Word16,
    time :: Time,
    timeDiff :: Time,
    windowSize :: Word32,
    seqNum :: SeqNum,
    ackNum :: AckNum,
    payload :: ByteString
  } deriving (Show, Eq)

defPacket = Packet ST_FIN 0 0 0 0 0 0 0 0 ""

-- CONSTANTS
headerSize = BS.length $ DS.encode $ Packet ST_DATA 0 0 0 0 0 0 0 0 ""
defWindowSize = 500

-- total size of the receive buffer
-- TODO: figure out what to do with this; currently not used
recvBufferSize = 10000

-- how many bytes to read in a socket receive operation
-- TODO: really need to have some logic behind this value
-- currently it's the value found in libtorrent
-- if a packet is bigger than this protocol logic fails
-- since packet
recvSize = 4096
versionNum = 1

defResendTimeout = 5 * 10 ^ 5

packetSize p = headerSize + (BS.length $ payload p)

-- how many duplicate acks to receive before marking a packet as lost
lossDupAcks = 3

-- the connection returned
data Connection = Conn {
    send :: ByteString -> IO ()
  , recv :: Int -> IO ByteString
  , close :: IO ()
  }

data ConnStage = CS_SYN_SENT | CS_CONNECTED | ERROR deriving (Show, Eq)

data ConnData = ConnData {
    connState :: TVar ConnState
  , inBuf :: TVar (BankersDequeue ByteString) 
  , outBuf :: TVar (BankersDequeue Packet)
  , connSocket :: Socket
  , sockSend :: SockSend 
  , connIdRecv ::ConnectionId
  , connIdSend ::ConnectionId
  }


data ConnState = ConnState {
    connSeqNum :: SeqNum
  , connAckNum :: AckNum
  , maxWindow :: Word32
  , peerMaxWindow :: Word32 -- window size advertised by peer
  , replyMicro :: Time
  , connStage :: ConnStage
  , dupAcks :: Int
  , lastAckRecv :: AckNum
  }

data UTPException = FailedHandshake deriving (Show, Typeable)
instance Exception UTPException

{- server call
  not optimized to handle a large number of calls
  good enough to handle p2p apps with something 
  like at most 100 connections

  TODO: figure out how to graciously kill this.

  It has a thread listening on the socket and dispatching
  incoming udp packets to the responsible threads

  Potential optimization: to fetch the right socket distribute everything 
  over a fixed size hashtable with TVars containing sockets (lists, trees)
  no single contention place in that situation
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
            writeTChan inChan packet -- push the first message in the chan
            return inChan
          (serverHandshake inChan sock sockAddr >>= handle sockAddr)
          `finally`
          (atomically $ modifyTVar connMapVar (Map.delete sockAddr) )
        return ()

serverHandshake :: TChan Packet -> Socket -> SockAddr -> IO Connection
serverHandshake packChan sock sockAddr  = do
  packet <- atomically $ readTChan packChan 
  when (packetType packet /= ST_SYN) $ throwIO FailedHandshake

  debugM utplogger $ "received syn packet " ++ (show packet)

  g <- newStdGen
  let randNum = P.head $ (randoms g :: [Word16])
  conn <- initConn (connId packet) (connId packet + 1) randNum (seqNum packet)
          sock (\bs -> NSB.sendTo sock bs sockAddr)

  -- send ack for syn
  sendPacket (makePacket ST_STATE "" conn) conn

  -- only the first ack increments the sequence number
  atomically $ modifyTVar (connState conn) (\s -> s {connSeqNum = connSeqNum s + 1})

  let recvF = atomically $ readTChan packChan
  forkIO $ setInterval defResendTimeout $ resendOutgoing conn
  -- run recv thread
  forkIO $ forever (recvF >>= recvIncoming conn)
  makeConnection conn

-- what a client calls
utpConnect :: Socket -> IO Connection
utpConnect sock = do
  g <- newStdGen
  let randId = P.head $ (randoms g :: [Word16])

  conn <- initConn randId (randId + 1) 1 0 sock (NSB.send sock)

  debugM utplogger "sending syn"
  sendPacket (makePacket ST_SYN "" conn) conn

  -- run resend thread
  forkIO $ setInterval defResendTimeout $ resendOutgoing conn

  let recvF = fmap P.fst $ recvPacket (connSocket conn) recvSize

  -- block here until syn-ack stage is done
  fstAck <- ackRecv recvF conn
  -- handshake is succseful

  atomically $ modifyTVar (connState conn)
           (\s -> s {connStage = CS_CONNECTED, connAckNum = seqNum fstAck})

  -- run recv thread
  forkIO $ forever (recvF >>= recvIncoming conn)
  
  makeConnection conn
   
-- loops until it reads a valid packet
recvPacket sock recvSize = fmap unwrapLeft $ runEitherT $ forever $ do
    (msg, src) <- liftIO $ NSB.recvFrom sock recvSize
    case (DS.decode msg :: Either String Packet) of
      -- ignore and keep looping listening for packets
      Left err -> liftIO $ infoM utplogger "non-utp package received"

      Right packet -> left (packet, src) -- exit loop

makeConnection :: ConnData -> IO Connection
makeConnection conn = do
  let send = \payload -> sendPacket (makePacket ST_DATA payload conn) conn
  return $ Conn send (recvPub conn) (return ())

recvPub conn len = do
  atomically $ do
    incoming <- readTVar (inBuf conn)
    when (DQ.length incoming == 0) retry -- empty buffer
    let (bytes, rest) =  takeBytes len incoming
    writeTVar (inBuf conn) rest
    return bytes

makePacket pType load conn
  = defPacket {packetType = pType, payload = load, connId = connIdRecv conn
           , version = versionNum, extensions = 0}

{-
  this function sets the following fields of packet
    time :: Time
    timeDiff :: Time
    windowSize :: Word32
    seqNum :: SeqNum
    ackNum :: AckNum
-} 
sendPacket packet conn = do
  debugM  utplogger $ "building packet..."
 
  sequenced <- atomically $ do
    state <- readTVar $ connState conn 
    out <- readTVar $ outBuf conn
    let currSeq = connSeqNum state
    inB <- readTVar $ inBuf conn
    let sequenced = packet {seqNum = currSeq, ackNum = connAckNum state
                    , timeDiff = replyMicro state
                    , windowSize = fromIntegral $ recvBufferSize -  dqSize inB (BS.length)}

    -- acks are not buffered and don't inc sequence number
    when (packetType sequenced /= ST_STATE) $ do
      -- if there is no space in the buffer block and retry later
      when (maxWindow state < fromIntegral ((packetSize sequenced) + dqSize out packetSize))
        retry
      writeTVar (outBuf conn) (pushBack out sequenced)
      writeTVar (connState conn) (state {connSeqNum = currSeq + 1})
    return sequenced

  micros <- getTimeMicros 
  let timestamped = sequenced {time = micros}
  debugM  utplogger $ "sending packet..." ++ (show timestamped)
  (sockSend conn) (DS.encode $ timestamped)        
  return ()
 
ackRecv recv conn = do
  packet <- recv
  case (packetType packet) of
    ST_STATE -> handleAck conn packet >> return packet
    _ -> do
      errorM utplogger "got something other than ack"
      throwIO FailedHandshake


handleAck conn packet = do
  lostPacket <- atomically $ do
    modifyTVar (outBuf conn) (P.snd . (dqTakeWhile ((<= ackNum packet) . seqNum)))

    -- handle duplicate acks
    state <- readTVar (connState conn)
    
    let dup = lastAckRecv state == ackNum packet
    let reachedLimit = dupAcks state + 1 >= lossDupAcks
    if' dup
      (if' reachedLimit 
        (modifyTVar (outBuf conn) (P.snd . popFront)) -- drop that packet
        (modifyTVar (connState conn) (\s -> s {dupAcks = dupAcks s + 1}) ))

      -- new ack coming in; reset state
      (modifyTVar (connState conn) (\s -> s {dupAcks = 0, lastAckRecv = ackNum packet}))
    return $ dup && reachedLimit -- return if packet loss happened
  when lostPacket $ errorM utplogger "lost packet!"
    

resendOutgoing conn = do
  outgoing <- atomically $ readTVar (outBuf conn)
  forM (dqToList outgoing) $ \p -> sockSend conn $ DS.encode p
  return ()

recvIncoming :: ConnData -> Packet ->  IO ()
recvIncoming conn packet = case packetType packet of
  ST_SYN -> do
    debugM utplogger "received syn packet"
    sendPacket (makePacket ST_STATE "" conn) conn
  ST_STATE -> handleAck conn packet
  ST_DATA -> do
    debugM utplogger $ "received data packet " ++ (show packet)
    inB <- atomically $ do
      stateNow <- readTVar $ connState conn
      when (connAckNum stateNow + 1 == seqNum packet) $ do
        modifyTVar (inBuf conn) (P.flip DQ.pushBack $ payload packet)
        modifyTVar (connState conn) (\s -> s {connAckNum = seqNum packet})
      readTVar (inBuf conn)

    debugM utplogger $ show inB
    sendPacket (makePacket ST_STATE "" conn) conn -- ack


initConn :: ConnectionId -> ConnectionId -> SeqNum
         -> AckNum -> Socket -> SockSend -> IO ConnData
initConn recvId sendId initSeqNum initAckNum sock send = do
  let initState = ConnState {connSeqNum = initSeqNum,
                            connAckNum = initAckNum,
                            connStage = CS_SYN_SENT,
                            maxWindow = defWindowSize,
                            peerMaxWindow = defWindowSize,
                            replyMicro = 0,
                            dupAcks = 0,
                            lastAckRecv = 0} 
 
  stateVar <- newTVarIO initState 
  inBufVar <- newTVarIO DQ.empty 
  outBufVar <- newTVarIO DQ.empty 
  return $ ConnData stateVar inBufVar outBufVar sock send recvId sendId 


-- PACKET SERIALIZATION

instance Serialize Packet where
  get = (\(t, v) -> Packet t v) <$> getTypeVersion <*> getWord8 <*> getWord16be
                                <*>  getWord32be <*> getWord32be <*> getWord32be
                                <*> getWord16be  <*> getWord16be <*> getRest
  -- assumes valid packet
  put Packet {..} = do
    putWord8 ((shiftL (fromJust $ P.lookup packetType $
                P.map swap packetTypeMap) 4) + version)
    putWord8 extensions 
    putWord16be connId
    putWord32be time
    putWord32be timeDiff 
    putWord32be windowSize
    putWord16be seqNum 
    putWord16be ackNum 
    putByteString payload

getTypeVersion = do
  byte <- getWord8 
  let packType = P.lookup (shiftR byte 4) packetTypeMap
  let version = shiftR (shiftL byte 4) 4
  case packType of 
    Just typeVal ->  return (typeVal, version)
    Nothing -> fail "unknown packet type"

getRest = remaining >>= getBytes


-- HELPERS

getTimeMicros = fmap (\n -> P.round $ n * 10 ^ 6) $ getPOSIXTime
setInterval t f = forever $ threadDelay t >> f


-- take a number of bytes from a dq containing bytestrings
takeBytes c b = (\(cs, dq) -> (BS.concat cs, dq) ) $ go c b
  where
   go count buf
     | DQ.length buf == 0 || count == 0  = ([], buf)
     | otherwise = if' (count >= topLen)
                     (top : chunks, rest) 
                     ([lastChunk], pushFront tail bsRest)
       where
         (Just top, tail) = popFront buf
         topLen = BS.length top
         (chunks, rest) = go (count - topLen) tail -- lazy 
         (lastChunk, bsRest) = BS.splitAt count top

-- takes first elems returning remaining DQ
dqTakeWhile :: Dequeue q => (a -> Bool) -> q a -> ([a], q a)
dqTakeWhile cond dq = case DQ.length dq of
  0 -> ([], dq)
  _ -> let (taken, rest) = dqTakeWhile cond (P.snd $ popFront dq) in 
         if' (cond $ fromJust $ DQ.first dq)
          ((fromJust $ DQ.first dq) : taken, rest) 
          ([], dq)

-- dumb summing up of sizes
-- optimize by keeping track of size on removal and insertion
dqSize :: Dequeue q => q a -> (a -> Int)  -> Int
dqSize dq len = P.sum $ P.map len $ dqToList dq

dqToList dq = DQ.takeFront (DQ.length dq) dq

if' c a b = if c then a else b
unwrapLeft (Left x) = x
toWord32 :: [Word8] -> Word32
toWord32 = P.foldr (\o a -> (a `shiftL` 8) .|. fromIntegral o) 0

-- manual debugging Setup

-- create 2 UDP sockets tunnel through them

echoPort = 9901

maxline = 1500

clientUTP =  do
  updateGlobalLogger utplogger (setLevel DEBUG)
  withSocketsDo $ do
    debugM utplogger "running"
    sock <- socket AF_INET Datagram 0
    NS.connect sock (SockAddrInet echoPort (toWord32 [127, 0, 0, 1]))
    conn <- utpConnect sock
    Network.UTP.send conn  "hello"
    debugM utplogger "sent message "
    resp <- Network.UTP.recv conn 2
    debugM utplogger $ "got echo response " ++ (show resp)


utpechoserver :: IO ()
utpechoserver = do
  updateGlobalLogger utplogger (setLevel DEBUG)
  withSocketsDo $ do
    sock <- socket AF_INET Datagram 0
    bindSocket sock (SockAddrInet echoPort iNADDR_ANY)
    utpListen sock  utpsocketEcho


utpsocketEcho :: SockAddr -> Connection -> IO ()
utpsocketEcho addr conn = forever $ do
           mesg  <- Network.UTP.recv conn maxline
           debugM utplogger $ "got message from utp socket " ++ (show mesg)
           send_count <- Network.UTP.send conn mesg 
           return ()
--           send_count <- NS.sendTo sock mesg client


