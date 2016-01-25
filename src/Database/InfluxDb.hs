{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

module Database.InfluxDb
   ( Duration(..)
   , FieldKey
   , FieldSet
   , FieldValue
   , InfluxDbClient
   , Point(..)
   , RetentionPolicy(..)
   , Series(..)
   , ToPoint(..)
   , ToSeriesPoint(..)
   , newClient
   , newClientWithSettings
   , pointConsumer
   ) where

import Control.Exception (Exception)
import Control.Monad (when)
import Control.Monad.Catch (throwM)
import Control.Monad.Trans.Resource (MonadResource)

import Data.Conduit (Consumer, await)
import Data.Int (Int64)
import Data.Monoid ((<>))
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8)
import Data.Typeable (Typeable)
import Data.Vector (Vector)
import Data.Word (Word64)

import qualified Blaze.ByteString.Builder as B

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Map.Strict as M
import qualified Data.Vector as V

import qualified Network.HTTP.Conduit as N
import qualified Network.HTTP.Types.Status as N
import qualified Network.HTTP.Types.Method as N

import qualified Text.Show.ByteString as TB

data RetentionPolicy = RetentionPolicy
    { rpName :: !Text
    , rpDuration :: !Duration 
    , rpReplication :: !(Maybe Int)
    }

data Duration = Hours !Int | Days !Int | Weeks !Int | INF

data Series = Series
    { sMeasurement :: !Text
    , sTagSet :: !(M.Map Text Text)
    }

type FieldKey = Text
type FieldValue = Text
type FieldSet = Vector (FieldKey, FieldValue)

data Point = Point
    { pTimestamp :: !(Maybe Word64)
    , pFields :: !FieldSet
    }

instance ToPoint Point where
    toPoint = id

class ToPoint a where
    toPoint :: a -> Point

class ToSeriesPoint a where
    toSeriesPoint :: a -> (Series, Point)

data InfluxDbException = IngestionError String deriving (Typeable, Show)

instance Exception InfluxDbException

data InfluxDbClient = InfluxDb
    { icManager :: !N.Manager
    , icDefaultRequest :: !N.Request
    }

newClient :: String -> IO InfluxDbClient
newClient = newClientWithSettings N.tlsManagerSettings

newClientWithSettings :: N.ManagerSettings -> String -> IO InfluxDbClient
newClientWithSettings settings url = do
    req <- N.parseUrl url 
    mng <- N.newManager settings 
    return InfluxDb
        { icManager = mng 
        , icDefaultRequest = req
        }

--pointCollector :: MonadResource m => Int -> Conduit Point m (V.Vector Point)
--pointCollector !size = initLoop 
--    where
--        initLoop = liftIO (VM.unsafeNew size) >>= loop 0
--        loop ndx vec = await >>= maybe cleanup (writeVal ndx vec)
--            where cleanup = when (ndx > 0) $ 
--                     (liftIO . V.unsafeFreeze $ VM.unsafeTake ndx vec) >>= yield
--        writeVal !ndx vec p = do 
--            liftIO $ VM.unsafeWrite vec ndx p
--            if ndx' == size then
--                liftIO (V.unsafeFreeze vec) >>= yield >> initLoop
--            else loop 0 vec
--            where ndx' = ndx + 1

pointConsumer :: (MonadResource m, ToPoint p)
               => InfluxDbClient -- ^ The client to use for interacting with influxdb.
               -> Text -- ^ The database name.
               -> Int64 -- ^ The max number of bytes to send per request. 
               -> Series -- ^ The series to write points for.
               -> Consumer p m ()
pointConsumer client dbName size series = loop mempty 0
    where 
      (!prefCnt, !lnPrefix) =
          (fromIntegral $ BS.length pref + 1, B.insertByteString $ pref `BS.append` " ")
        where pref 
                | M.null (sTagSet series) = encodeUtf8 (sMeasurement series)
                | otherwise = BS.intercalate "," (encodeUtf8 (sMeasurement series) : tags)
                    where tags = map kvLBS $ M.toList (sTagSet series) 
      kvLBS (k, v) = BS.concat [encodeUtf8 k, "=", encodeUtf8 v]
      loop body !byteCnt = await >>= maybe 
                                    (when (byteCnt > 0) $ sendReq body byteCnt) 
                                    (bodyApp body byteCnt . toPoint)
      bodyApp body byteCnt p  
        | byteCnt' > size = sendReq body byteCnt >> loop pointBld cnt 
        | otherwise = loop (body <> pointBld) byteCnt' 
        where 
          dataLbs = (LBS.intercalate "," . map (LBS.fromStrict . kvLBS) . V.toList $ pFields p) <>
                    maybe mempty (mappend " " . TB.show) (pTimestamp p) <> "\n"
          pointBld = lnPrefix <> B.insertLazyByteString dataLbs
          !cnt = prefCnt + LBS.length dataLbs
          !byteCnt' = byteCnt + cnt
      !dbQueryString = "db=" `BS.append` encodeUtf8 dbName
      sendReq body byteCnt = do
          res <- N.http req' (icManager client)
          when (N.responseStatus res /= N.status204) $
            throwM . IngestionError . show $ N.responseStatus res
          where
            req' = (icDefaultRequest client) 
                    { N.method  = N.methodPost
                    , N.requestBody = N.RequestBodyBuilder byteCnt body
                    , N.path = "write"
                    , N.queryString = dbQueryString
                    }
