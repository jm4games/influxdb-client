{-# LANGUAGE BangPatterns #-}

module Database.InfluxDb.Types where

import Control.Monad (mzero)

import Data.Int (Int64)
import Data.Maybe (fromMaybe)
import Data.Scientific (Scientific, floatingOrInteger, toBoundedInteger)
import Data.Text (Text)
import Data.Word (Word64)

import TextShow (showt)

import qualified Data.Map.Strict as M
import qualified Data.Vector as V

import qualified Data.Aeson as A

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
type FieldValue = Maybe Text
type FieldSet = V.Vector (FieldKey, FieldValue)

data Point = Point
    { pTimestamp :: !(Maybe Word64)
    , pFields :: !FieldSet
    }
    deriving (Show)

instance ToPoint Point where
    toPoint = id

class ToPoint a where
    toPoint :: a -> Point

class ToSeriesPoint a where
    toSeriesPoint :: a -> (Series, Point)

data QueryResult =  QueryError !Text
                  | QueryResult (V.Vector SeriesResult)
                  deriving (Show)

data SeriesResult = SeriesResult Text (V.Vector Point) deriving (Show)

instance A.FromJSON QueryResult where
    parseJSON (A.Object o) =
       o A..:? "results" >>= \res ->
           case res of
               Just (A.Array a) -> QueryResult <$>
                   V.mapM (\x -> readArray (unwrapObj x) "series" >>= readSeries) a
               Just x -> error $ "Unexpected element: " ++ show x
               Nothing -> do
                   rErr <- o A..:? "error"
                   case rErr of
                      Just (A.String err) -> return $ QueryError err 
                      _ -> error "Unexpected result."
        where
            readSeries series
              | series == V.empty = return $ SeriesResult mempty V.empty
              | otherwise = do
                  let series1 = unwrapObj $ series V.! 0
                  name <- readText series1 "name" 
                  cols <- V.map unwrapText <$> readArray series1 "columns" 
                  let tIndex = V.findIndex ("time" ==) cols
                  points <- V.map (readPoint tIndex cols) <$> readArray series1 "values"  
                  return $ SeriesResult name points

            unwrapObj (A.Object x) = x 
            unwrapObj _ =  error "Unexpected json value (expected obj)."

            unwrapText (A.String s) = s 
            unwrapText _ =  error "Unexpected json value (expected text)."

            unwrapNumber (A.Number n) = n 
            unwrapNumber _ =  error "Unexpected json value (expected text)."

            readArray obj name = obj A..:? name >>= \x ->
                case x of
                    Just (A.Array a) -> return a 
                    Nothing -> return V.empty
                    _ -> error $ show name ++ " is not a array property."

            readText obj name = obj A..: name >>= \x ->
                case x of
                    A.String s -> return s 
                    _ -> error $ show name ++ " is not a text property."

            readPoint !tIndex cols (A.Array a) = Point
                { pTimestamp = fromMaybe (error "Invalid Timestamp") . toBoundedInteger 
                                . unwrapNumber . (V.!) a <$> tIndex
                , pFields = V.zipWith (\c b -> (c , maybeUnwrapText b)) cols a 
                }
            readPoint _ _ _ = error "Unexpected json type in value array."

            maybeUnwrapText val = 
                case val of
                    (A.String s) -> Just s
                    (A.Number n) -> Just . either showt showt $ 
                        (floatingOrInteger :: Scientific -> Either Double Int64) n
                    (A.Bool b) -> Just $ showt b
                    A.Null -> Nothing
                    _ -> error "Unexpected json value"

    parseJSON _ = mzero
