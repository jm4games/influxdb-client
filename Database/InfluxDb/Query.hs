{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeSynonymInstances #-}

module Database.InfluxDb.Query
  (
  -- * Types
    From
  , GroupBy
  , MultiSelect
  , Query(..)
  , SelectStatement
  , Target
  , WhereClause
  -- * Where Operators
  , (.&&.)
  , (.||.)
  , (.=.)
  , (.!=.)
  , (.>.)
  , (.<.)
  , (.=~.)
  , (.!~.)
  -- * Query Language
  -- ** Basics
  , select
  , from
  , from'
  , groupBy
  , where_
  -- ** Target Functions
  , allFields
  , count
  , field
  , fields
  , funMax
  , funMean
  , funMin
  , funSum
  , targets
  -- * Helpers
  , now
  ) where

import Data.ByteString (ByteString, intercalate)
import Data.ByteString.Builder (Builder, byteString, toLazyByteString)
import Data.ByteString.Lazy (toStrict)
import Data.List (foldl', intersperse)
import Data.Monoid ((<>))
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8)
import Data.Text.Read (double)

import TextShow (TextShow, showt)

import qualified Data.Text as T

data SelectStatement = Select
  { ssTarget  :: !Target
  , ssFrom    :: !From
  , ssWhere   :: !(Maybe WhereClause)
  , ssGroupBy :: !(Maybe GroupBy)
  }

instance Show SelectStatement where
  show = show . toByteString

instance Query SelectStatement where
  toByteString ss = toStrict . toLazyByteString $ selectBuilder
    <> maybe mempty (mappend "%20WHERE%20" . whereBuilder) (ssWhere ss)
    <> maybe mempty (mappend "%20GROUP%20BY%20" . groupByBuilder) (ssGroupBy ss)
    where
      selectBuilder = "SELECT%20" <> targetBuilder (ssTarget ss) <>
                      "%20FROM%20" <> fromBuilder (ssFrom ss)

type MultiSelect = [SelectStatement]

instance Query MultiSelect where
  toByteString = intercalate "%3B" . map toByteString

class Query q where
  toByteString :: q -> ByteString

data Target = Count  !Text
            | Field  !Text
            | Targets ![Target]
            | Max    !Text
            | Mean   !Text
            | Min    !Text
            | Sum    !Text

data From = FromShort Text
          | FromFull Text Text Text

class Expression e where
  fieldName :: e -> Text
  fieldValue :: e -> Text

data WhereClause = And      !WhereClause !WhereClause
                 | Or       !WhereClause !WhereClause
                 | Eq       !Text !Text
                 | Ne       !Text !Text
                 | Gt       !Text !Text
                 | Lt       !Text !Text
                 | Match    !Text !Text
                 | NotMatch !Text !Text

instance Expression WhereClause where
    fieldName (Gt f _) = f
    fieldName (Lt f _) = f
    fieldName (Eq f _) = f
    fieldName (Ne f _) = f
    fieldName (Match f _) = f
    fieldName (NotMatch f _) = f
    fieldName (And _ _) = error "'And' expression does not support field."
    fieldName (Or _ _) = error "'Or' expression does not support field."

    fieldValue (Gt _ v) = v
    fieldValue (Lt _ v) = v
    fieldValue (Eq _ v) = v
    fieldValue (Ne _ v) = v
    fieldValue (Match _ v) = v
    fieldValue (NotMatch _ v) = v
    fieldValue (And _ _) = error "'And' expression does not support value."
    fieldValue (Or _ _) = error "'Or' expression does not support value."

(.&&.) :: WhereClause -> WhereClause -> WhereClause
infixr 3 .&&.
a .&&. b = And a b

(.||.) :: WhereClause -> WhereClause -> WhereClause
infixr 3 .||.
a .||. b = Or a b

(.=.) :: TextShow a => Text -> a -> WhereClause
infix 4 .=.
a .=. b = Eq a (showTxt b)

(.!=.) :: TextShow a => Text -> a -> WhereClause
infix 4 .!=.
a .!=. b = Ne a (showTxt b)

(.>.) :: TextShow a => Text -> a -> WhereClause
infix 4 .>.
a .>. b = Gt a (showTxt b)

(.<.) :: TextShow a => Text -> a -> WhereClause
infix 4 .<.
a .<. b = Lt a (showTxt b)

(.=~.) :: TextShow a => Text -> a -> WhereClause
infix 4 .=~.
a .=~. b = Match a (showTxt b)

(.!~.) :: TextShow a => Text -> a -> WhereClause
infix 4 .!~.
a .!~. b = NotMatch a (showTxt b)

newtype GroupBy = GroupBy Text

groupBy :: SelectStatement -> Text -> SelectStatement
infix 1 `groupBy`
groupBy ss "" = ss { ssGroupBy = Nothing }
groupBy ss txt = ss { ssGroupBy = Just $ GroupBy txt }

groupByBuilder :: GroupBy -> Builder
groupByBuilder (GroupBy g) = byteString $ encodeUtf8 g

showTxt :: TextShow a => a -> Text
showTxt x
  | "\"" `T.isPrefixOf` txt = T.take (T.length txt - 2) $ T.drop 1 txt
  | otherwise = txt
  where txt = showt x

select :: Target -> From -> SelectStatement
select t f = Select
  { ssTarget = t
  , ssFrom = f
  , ssWhere = Nothing
  , ssGroupBy = Nothing
  }

from :: Text -> From
from = FromShort

from' :: Text -> Text -> Text -> From
from' = FromFull

allFields :: Target
allFields = Field "*"

field :: Text -> Target
field = Field

count :: Text -> Target
count = Count

funMean :: Text -> Target
funMean = Mean

funSum :: Text -> Target
funSum = Sum

funMax :: Text -> Target
funMax = Max

funMin :: Text -> Target
funMin = Min

fields :: [Text] -> Target
fields = Targets . map Field

targets :: [Target] -> Target
targets = Targets

where_ :: SelectStatement -> WhereClause -> SelectStatement
infix 2 `where_`
where_ ss@Select { ssWhere = Nothing } w = ss { ssWhere = Just w }
where_ ss@Select { ssWhere = Just x } w = ss { ssWhere = Just $ And x w }

textBuilder :: Text -> Builder
textBuilder = byteString . encodeUtf8

targetBuilder :: Target -> Builder
targetBuilder (Field f) = textBuilder f
targetBuilder (Count f) = "COUNT%28" <> textBuilder f <> "%29"
targetBuilder (Max f)   = "MAX%28"   <> textBuilder f <> "%29"
targetBuilder (Min f)   = "MIN%28"   <> textBuilder f <> "%29"
targetBuilder (Mean f)  = "MEAN%28"  <> textBuilder f <> "%29"
targetBuilder (Sum f)   = "SUM%28"   <> textBuilder f <> "%29"
targetBuilder (Targets t) = foldl' (<>) mempty . intersperse "%2C" $ map targetBuilder t

fromBuilder :: From -> Builder
fromBuilder (FromShort t) = textBuilder t
fromBuilder (FromFull db ret m) =
  textBuilder db <> byteString ".\"" <> textBuilder ret <> "\"." <> textBuilder m

whereBuilder :: WhereClause -> Builder
whereBuilder (NotMatch a b) = textBuilder a <> "%20!~%20%27" <> textBuilder b <> "%27"
whereBuilder (And a b) = "%28" <> whereBuilder a <> "%29%20AND%20%28" <> whereBuilder b <> "%29"
whereBuilder (Or a b) =  "%28" <> whereBuilder a <> "%29%20OR%20%28" <> whereBuilder b <> "%29"
whereBuilder op = textBuilder (fieldName op) <> prefix op <> textBuilder (fieldValue op) <> suffix
  where
    !isNum = either (const False) (const True) . double $ fieldValue op
    prefix (Gt _ _) | isNum = "%20%3E%20"
                    | otherwise = "%20%3E%20%27"
    prefix (Lt _ _) | isNum = "%20%3C%20"
                    | otherwise = "%20%3C%20%27"
    prefix (Eq _ _) | isNum = "%20%3D%20"
                    | otherwise = "%20%3D%20%27"
    prefix (Ne _ _) | isNum = "%20!%3D%20"
                    | otherwise = "%20!%3D%20%27"
    prefix (Match _ _) | isNum = "%20%3D~%20"
                       | otherwise = "%20%3D~%20%27"
    prefix (NotMatch _ _) | isNum = "%20!~%20"
                          | otherwise = "%20!~%20%27"
    prefix _ = error "Unsupported expression."
    suffix | isNum = ""
           | otherwise = "%27"

now :: Text
now = "now()"
