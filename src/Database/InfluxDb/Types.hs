module Database.InfluxDb.Types where

import Control.Monad (mzero)

import Data.Text (Text)
import Data.Word (Word64)

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
type FieldValue = Text
type FieldSet = V.Vector (FieldKey, FieldValue)

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

data QueryResult =  QueryError !Text
                  | QueryResults [SeriesResult]

data SeriesResult = SeriesResult Text [Point]

instance A.FromJSON QueryResult where
    parseJSON (A.Object o) = do
       res <- o A..:? "results"
       case res of
           Just (A.Array a) ->
              case a V.! 0 of
                 (A.Object os) -> os A..: "series" >>= \s ->
                   case s of
                       Just (A.Array sa) -> undefined
                       _ -> error "Unexpected series, array not found."
                 _ -> error "Unexpected series result."
           Just x -> error $ "Unexpected element: " ++ show x
           Nothing -> do
               rErr <- o A..:? "error"
               case rErr of
                  Just (A.String err) -> return $ QueryError err 
                  Nothing -> error "Unexpected result."
       -- where
        --    parseRoot =  

    parseJSON _ = mzero
