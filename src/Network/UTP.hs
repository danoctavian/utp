{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

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

-- bittorrent utp protocol
proto = undefined

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
-}


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

packetSize p = headerSize + (BS.length $ payload p)

-- the connection returned
data Connection = Conn {
    send :: ByteString -> IO ()
  , recvLen :: Int -> IO (ByteString, Int)
  , recv :: Int -> IO ByteString
  }

{-
data Connection = Conn {
     connIdRecv :: ConnectionId        
   , connIdSend :: ConnectionId        
  }
-}

data ConnStage = CS_SYN_SENT | CS_CONNECTED deriving (Show, Eq)

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
dqSize dq len = P.sum $ P.map len $ DQ.takeFront (DQ.length dq) dq

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
  micros <- fmap (\n -> P.round $ n * 10 ^ 6) $ liftIO $ getPOSIXTime
  liftIO $ NSB.send (connSocket conn) (DS.encode $ sequenced {time = micros})        
  return ()
 

udpToUTPClient :: MonadIO m => Socket -> m Connection
udpToUTPClient sock = do
 g <- liftIO $ newStdGen
 let randId = P.head $ (randoms g :: [Word16])
 let connState = ConnState {connSeqNum = 1,
                           connAckNum = 0,
                           connStage = CS_SYN_SENT,
                           maxWindow = defWindowSize,
                           peerMaxWindow = defWindowSize,
                           replyMicro = 0}

 stateVar <- liftIO $ newTVarIO connState
 inBufVar <- liftIO $ newTVarIO DQ.empty 
 outBufVar <- liftIO $ newTVarIO DQ.empty 
 let connData = ConnData stateVar inBufVar outBufVar sock randId (randId + 1)


 return $ Conn (\bs -> return ()) (\n -> return ("", 0)) (\n -> return "")

 {- do
  send sock ""
  resp <- recv sock headerLen 
  -}

if' c a b = if c then a else b
