{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeSynonymInstances #-}

module Database.InfluxDb.Query
  ( 
  -- * Types  
    From
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

import TextShow (TextShow, showt)

import qualified Data.Text as T

data SelectStatement = Select
  { ssTarget :: !Target
  , ssFrom :: !From
  , ssWhere :: !(Maybe WhereClause)
  }

instance Show SelectStatement where
  show = show . toByteString

instance Query SelectStatement where
  toByteString ss = toStrict . toLazyByteString $ selectBuilder
    <> maybe mempty (mappend "%20WHERE%20" . whereBuilder) (ssWhere ss)
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

data WhereClause = And      !WhereClause !WhereClause
                 | Or       !WhereClause !WhereClause
                 | Eq       !Text !Text
                 | Ne       !Text !Text 
                 | Gt       !Text !Text
                 | Lt       !Text !Text 
                 | Match    !Text !Text 
                 | NotMatch !Text !Text

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
infix 1 `where_`
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
whereBuilder (Eq a b) =       textBuilder a <> "%20%3D%20%27"  <> textBuilder b <> "%27"
whereBuilder (Gt a b) =       textBuilder a <> "%20%3E%20%27"  <> textBuilder b <> "%27"
whereBuilder (Lt a b) =       textBuilder a <> "%20%3C%20%27"  <> textBuilder b <> "%27"
whereBuilder (Ne a b) =       textBuilder a <> "%20!%3D%20%27" <> textBuilder b <> "%27"
whereBuilder (Match a b) =    textBuilder a <> "%20%3D~%20%27" <> textBuilder b <> "%27"
whereBuilder (NotMatch a b) = textBuilder a <> "%20!~%20%27" <> textBuilder b <> "%27"
whereBuilder (And a b) = "%28" <> whereBuilder a <> "%29%20AND%20%28" <> whereBuilder b <> "%29"
whereBuilder (Or a b) =  "%28" <> whereBuilder a <> "%29%20OR%20%28" <> whereBuilder b <> "%29"

now :: Text
now = "now()"
