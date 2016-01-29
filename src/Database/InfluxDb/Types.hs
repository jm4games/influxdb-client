module Database.InfluxDb.Types where

import Control.Monad (mzero)

import Data.Int (Int64)
import Data.Scientific (Scientific, floatingOrInteger)
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
                   V.mapM (\x -> readArray (valAsObj x) "series" >>= readSeries) a
               Just x -> error $ "Unexpected element: " ++ show x
               Nothing -> do
                   rErr <- o A..:? "error"
                   case rErr of
                      Just (A.String err) -> return $ QueryError err 
                      _ -> error "Unexpected result."
        where
            readSeries series = do
                let series1 = valAsObj $ series V.! 0
                name <- readText series1 "name" 
                cols <- V.map valAsText <$> readArray series1 "columns" 
                let tIndex = V.findIndex ((==) "time" . valAsText)
                points <- V.map (readPoint tIndex cols) <$> readArray series1 "values"  
                return $ SeriesResult name points

            valAsObj (A.Object x) = x 
            valAsObj _ =  error "Unexpected json value (expected obj)."

            valAsText (A.String s) = s 
            valAsText _ =  error "Unexpected json value (expected text)."

            readArray obj name = obj A..: name >>= \x ->
                case x of
                    A.Array a -> return a 
                    _ -> error $ show name ++ " is not a array property."

            readText obj name = obj A..: name >>= \x ->
                case x of
                    A.String s -> return s 
                    _ -> error $ show name ++ " is not a text property."

            readPoint _ cols (A.Array a) = Point
                { pTimestamp = Nothing
                , pFields = V.zipWith (\c b -> (c , valAsMaybeText b)) cols a 
                }
            readPoint _ _ _ = error "Unexpected json type in value array."

            valAsMaybeText val = 
                case val of
                    (A.String s) -> Just s
                    (A.Number n) -> Just . either showt showt $ 
                        (floatingOrInteger :: Scientific -> Either Double Int64) n
                    (A.Bool b) -> Just $ showt b
                    A.Null -> Nothing
                    _ -> error "Unexpected json value"

    parseJSON _ = mzero
