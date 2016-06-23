{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}

module Database.InfluxDb
   ( module Database.InfluxDb.Types
   , InfluxDbClient
   , newClient
   , newClientWithSettings
   , pointConsumer
   , query
   , query'
   , rawQuery
   , rawQuery'
   , streamQuery) where

import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (MVar, newEmptyMVar, takeMVar, putMVar)
import Control.Exception (Exception)
import Control.Monad (unless, when, void)
import Control.Monad.Catch (throwM)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Resource (MonadResource, runResourceT)

import Data.Aeson (Value, parseJSON)
import Data.Aeson.Parser(value, value')
import Data.Aeson.Types(Result(..), parse)
import Data.Attoparsec.ByteString (Parser)
import Data.Conduit (($$+-), Consumer, Producer, await, yield)
import Data.Conduit.Attoparsec (sinkParser)
import Data.Int (Int64)
import Data.Monoid ((<>))
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8)
import Data.Typeable (Typeable)

import Database.InfluxDb.Types
import Database.InfluxDb.Query (MultiSelect, Query, toByteString)

import qualified Blaze.ByteString.Builder as B

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Map.Strict as M
import qualified Data.Vector as V

import qualified Network.HTTP.Conduit as N
import qualified Network.HTTP.Types.Status as N
import qualified Network.HTTP.Types.Method as N

import qualified Text.Show.ByteString as TB

data InfluxDbException = IngestionError String
                       | ParseError String
                       deriving (Typeable, Show)

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
      kvLBS' (k, Just v) = BS.concat [encodeUtf8 k, "=", encodeUtf8 v]
      kvLBS' (_, Nothing) = ""
      loop body !byteCnt = await >>= maybe
                                    (when (byteCnt > 0) $ sendReq body byteCnt)
                                    (bodyApp body byteCnt . toPoint)
      bodyApp body byteCnt p
        | byteCnt' > size = sendReq body byteCnt >> loop pointBld cnt
        | otherwise = loop (body <> pointBld) byteCnt'
        where
          dataLbs = (LBS.intercalate "," . map (LBS.fromStrict . kvLBS') . V.toList $ pFields p) <>
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
                    , N.path = "/write"
                    , N.queryString = dbQueryString
                    }

rawQuery :: MonadResource m => InfluxDbClient -> Text -> Text -> m QueryResult
rawQuery client db = rawQueryInternal value client db . encodeUtf8

rawQuery' :: MonadResource m => InfluxDbClient -> Text -> Text -> m QueryResult
rawQuery' client db = rawQueryInternal value' client db . encodeUtf8

query :: (MonadResource m, Query q)
      => InfluxDbClient
      -> Text
      -> q
      -> m QueryResult
query client db = rawQueryInternal value client db . toByteString

query' :: (MonadResource m, Query q)
       => InfluxDbClient
       -> Text
       -> q
       -> m QueryResult
query' client db = rawQueryInternal value' client db . toByteString

streamQuery :: MonadResource m
            => InfluxDbClient
            -> Text
            -> MultiSelect
            -> Producer m QueryResult
streamQuery _ _ [] = return ()
streamQuery client db qs = do
  let (nxt, rest) = splitAt batchSize qs
  resRef <- liftIO $ newEmptyMVar >>= (\x -> fetchData nxt x >> return x)
  resProducer resRef rest
  where
    batchSize = 5
    resProducer dataVar [] = liftIO (takeMVar dataVar) >>= yield >> return ()
    resProducer dataVar queries = do
      myData <- liftIO $ takeMVar dataVar
      let (nxt, rest) = splitAt batchSize queries
      unless (null nxt) . liftIO $ fetchData nxt dataVar
      yield myData
      resProducer dataVar rest
    fetchData :: MultiSelect -> MVar QueryResult -> IO ()
    fetchData vals dataVar = void . forkIO . runResourceT $
      rawQueryInternal value' client db (toByteString vals) >>= (liftIO . putMVar dataVar)

rawQueryInternal :: MonadResource m
                 => Parser Value
                 -> InfluxDbClient
                 -> Text
                 -> BS.ByteString
                 -> m QueryResult
rawQueryInternal parser client db qry = do
    res <- N.http req (icManager client)
    x <- N.responseBody res $$+- sinkParser parser
    case parse parseJSON x of
        Success a -> return a
        Error err -> throwM $ ParseError err
    where
      !dbString = "db=" `BS.append` encodeUtf8 db
      req = (icDefaultRequest client)
                { N.method = N.methodGet
                , N.path = "/query"
                , N.queryString = BS.concat [dbString, "&epoch=ns&q=", qry]
                }
