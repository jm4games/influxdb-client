{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

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
   , sendPoints
   , sendSeriesPoints
   , streamSeriesResults
   , streamQuery
   ) where

import Control.Concurrent.Lifted (fork)
import Control.Concurrent.MVar (MVar, newEmptyMVar, takeMVar, putMVar)
import Control.Exception (Exception, throwIO, SomeException)
import Control.Exception.Lifted (try)
import Control.Monad (unless, when, void)
import Control.Monad.Catch (throwM)
import Control.Monad.Morph (lift)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Control (MonadBaseControl)
import Control.Monad.Trans.Resource (MonadResource)

import Data.Aeson (Value, parseJSON)
import Data.Aeson.Parser(value, value')
import Data.Aeson.Types(Result(..), parse)
import Data.Attoparsec.ByteString (Parser)
import Data.Conduit (($$+-), Consumer, Producer, await, yield)
import Data.Conduit.Attoparsec (sinkParser)
import Data.Int (Int64)
import Data.List (foldl')
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
                       | QueryException Text
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
    req <- N.parseRequest url
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
pointConsumer client !dbName !size !series = loop mempty 0
    where
      (!prefCnt, !lnPrefix) = seriesToBuilder series
      sendReq = sendWriteRequest client dbName
      loop body !byteCnt = await >>= maybe
                                    (when (byteCnt > 0) $ sendReq body byteCnt)
                                    (bodyAppend body byteCnt . toPoint)
      bodyAppend body byteCnt p
        | byteCnt' > size = sendReq body byteCnt >> loop pointBld cnt
        | otherwise = loop (body <> pointBld) byteCnt'
        where
          dataLbs = pointToByteString p
          pointBld = lnPrefix <> B.insertLazyByteString dataLbs
          !cnt = prefCnt + LBS.length dataLbs
          !byteCnt' = byteCnt + cnt

seriesToBuilder :: Series -> (Int64, B.Builder)
seriesToBuilder series = (fromIntegral $ BS.length pref + 1, B.insertByteString $ pref `BS.append` " ")
  where
    pref | M.null (sTagSet series) = encodeUtf8 (sMeasurement series)
         | otherwise = BS.intercalate "," (encodeUtf8 (sMeasurement series) : tags)
             where tags = map kvLBS $ M.toList (sTagSet series)
    kvLBS (k, v) = BS.concat [encodeUtf8 k, "=", encodeUtf8 v]

pointToByteString :: Point -> LBS.ByteString
pointToByteString p =
  (LBS.intercalate "," . map (LBS.fromStrict . fieldToByteString) . V.toList $ pFields p) <>
  maybe mempty (mappend " " . TB.show) (pTimestamp p) <> "\n"
  where
    fieldToByteString (k, Just v) = BS.concat [encodeUtf8 k, "=", encodeUtf8 v]
    fieldToByteString (_, Nothing) = ""

sendWriteRequest :: MonadResource m => InfluxDbClient -> Text -> B.Builder -> Int64 -> m ()
sendWriteRequest client dbName body bodySize = do
  res <- N.http req' (icManager client)
  when (N.responseStatus res /= N.status204) $
    throwM . IngestionError . show $ N.responseStatus res
  where
    !dbQueryString = "db=" `BS.append` encodeUtf8 dbName `BS.append` "&precision=n"
    req' = (icDefaultRequest client)
            { N.method  = N.methodPost
            , N.requestBody = N.RequestBodyBuilder bodySize body
            , N.path = "/write"
            , N.queryString = dbQueryString
            }

sendPoints :: (MonadResource m, ToPoint p)
           => InfluxDbClient -- ^ The client to use for interacting with influxdb.
           -> Text -- ^ The database name.
           -> Series
           -> [p]
           -> m Int64
sendPoints client !dbName series points = sendWriteRequest client dbName body bodySize >> return totalCnt
  where
    (!bodySize, body, !totalCnt) = foldl' appendPoint (0, mempty, 0) points
    (!prefSize, !pref) = seriesToBuilder series
    appendPoint (!bSize, bdy, !pCnt) p = (bSize + cnt, bdy <> pointBld, pCnt + 1)
      where
        pointLbs = pointToByteString $ toPoint p
        pointBld = pref <> B.insertLazyByteString pointLbs
        !cnt = prefSize + LBS.length pointLbs

sendSeriesPoints :: (MonadResource m, ToSeriesPoint p)
                 => InfluxDbClient -- ^ The client to use for interacting with influxdb.
                 -> Text -- ^ The database name.
                 -> [p]
                 -> m Int64
sendSeriesPoints client !dbName points = sendWriteRequest client dbName body bodySize >> return totalCnt
  where
    (!bodySize, body, !totalCnt) = foldl' appendPoint (0, mempty, 0) points
    appendPoint (!bSize, bdy, !pCnt) val = (bSize + cnt, bdy <> pointBld, pCnt + 1)
      where
        (series, p) = toSeriesPoint val
        (!prefSize, !pref) = seriesToBuilder series
        dataLbs = pointToByteString p
        pointBld = pref <> B.insertLazyByteString dataLbs
        !cnt = prefSize + LBS.length dataLbs

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

streamQuery :: (MonadResource m, MonadBaseControl IO m)
            => InfluxDbClient
            -> Text
            -> MultiSelect
            -> Producer m QueryResult
streamQuery _ _ [] = return ()
streamQuery client db qs = streamQueryInternal 5 qs (rawQueryInternal value' client db)

streamSeriesResults :: (MonadResource m, MonadBaseControl IO m, FromSeriesResult r)
                    => InfluxDbClient
                    -> Text
                    -> MultiSelect
                    -> Producer m (V.Vector r)
streamSeriesResults _ _ [] = return ()
streamSeriesResults client db qs = streamQueryInternal 5 qs (rawPointQueryInternal value' client db)

streamQueryInternal :: forall a m. (MonadResource m, MonadBaseControl IO m)
                    => Int -- ^ Batch size.
                    -> MultiSelect
                    -> (BS.ByteString -> m a)
                    -> Producer m a
streamQueryInternal batchSize qs runQry = do
  let (nxt, rest) = splitAt batchSize qs
  mvar <- liftIO newEmptyMVar
  lift $ fetchData nxt mvar
  resProducer mvar rest
  where
    resProducer dataVar [] = liftIO (takeMVar dataVar) >>= either throwM yield >> return ()
    resProducer dataVar queries = do
      result <- liftIO $ takeMVar dataVar
      case result of
        Right myData -> do
          let (nxt, rest) = splitAt batchSize queries
          unless (null nxt) . lift $ fetchData nxt dataVar
          yield myData
          resProducer dataVar rest
        Left e -> throwM e
    fetchData :: MultiSelect -> MVar (Either SomeException a) -> m ()
    fetchData vals dataVar = void . fork $
      try (runQry (toByteString vals)) >>= (liftIO . putMVar dataVar)

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

rawPointQueryInternal :: (MonadResource m, FromSeriesResult r)
                      => Parser Value
                      -> InfluxDbClient
                      -> Text
                      -> BS.ByteString
                      -> m (V.Vector r)
rawPointQueryInternal parser client db qry = do
  res <- rawQueryInternal parser client db qry
  case res of
    QueryError err -> liftIO (throwIO $ QueryException err)
    QueryResult rs -> return . V.map fromSeriesResult $
                        V.filter (\(SeriesResult (Series name _) _) -> name /= "") rs
