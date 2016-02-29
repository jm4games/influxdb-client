{-# LANGUAGE OverloadedStrings #-}

module Database.InfluxDb.Query
  ( 
  -- * Types  
    From
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
  , where'
  -- ** Target Functions
  , countT
  , fieldT
  , maxT
  , meanT
  , minT
  , sumT
  -- * Conversion
  , toByteString
  -- * Helpers
  , allT
  , now
  ) where

import Data.ByteString (ByteString)
import Data.ByteString.Builder (Builder, byteString, toLazyByteString)
import Data.ByteString.Lazy (toStrict)
import Data.List (foldl', intersperse)
import Data.Monoid ((<>))
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8)

import TextShow (TextShow, showt)

data SelectStatement = Select
  { ssTarget :: !Target
  , ssFrom :: !From
  , ssWhere :: !(Maybe WhereClause)
  }

instance Show SelectStatement where
  show = show . toByteString

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
a .=. b = Eq a (showt b)

(.!=.) :: TextShow a => Text -> a -> WhereClause
infix 4 .!=.
a .!=. b = Ne a (showt b)

(.>.) :: TextShow a => Text -> a -> WhereClause
infix 4 .>.
a .>. b = Gt a (showt b)

(.<.) :: TextShow a => Text -> a -> WhereClause
infix 4 .<.
a .<. b = Lt a (showt b)

(.=~.) :: TextShow a => Text -> a -> WhereClause
infix 4 .=~.
a .=~. b = Match a (showt b)

(.!~.) :: TextShow a => Text -> a -> WhereClause
infix 4 .!~.
a .!~. b = NotMatch a (showt b)

select :: Target -> From -> SelectStatement
select t f = Select
  { ssTarget = t
  , ssFrom = f
  , ssWhere = Nothing
  }

allT :: Target
allT = Field "*"

from :: Text -> From
from = FromShort

from' :: Text -> Text -> Text -> From
from' = FromFull

fieldT :: Text -> Target
fieldT = Field

countT :: Text -> Target
countT = Count

meanT :: Text -> Target
meanT = Mean

sumT :: Text -> Target
sumT = Sum

maxT :: Text -> Target
maxT = Max

minT :: Text -> Target
minT = Min

where' :: SelectStatement -> WhereClause -> SelectStatement
infix 1 `where'`
where' ss@Select { ssWhere = Nothing } w = ss { ssWhere = Just w }
where' ss@Select { ssWhere = Just x } w = ss { ssWhere = Just $ And x w }

toByteString :: SelectStatement -> ByteString
toByteString ss = toStrict . toLazyByteString $ selectBuilder
  <> maybe mempty (mappend " WHERE " . whereBuilder) (ssWhere ss)
  where
    selectBuilder = "SELECT " <> targetBuilder (ssTarget ss) <>
                    " FROM " <> fromBuilder (ssFrom ss) 

textBuilder :: Text -> Builder
textBuilder = byteString . encodeUtf8

targetBuilder :: Target -> Builder
targetBuilder (Field f) = textBuilder f 
targetBuilder (Count f) = "COUNT(" <> textBuilder f <> ")"
targetBuilder (Max f) = "MAX(" <> textBuilder f <> ")"
targetBuilder (Min f) = "MIN(" <> textBuilder f <> ")"
targetBuilder (Mean f) = "MEAN(" <> textBuilder f <> ")"
targetBuilder (Sum f) = "SUM(" <> textBuilder f <> ")"
targetBuilder (Targets t) = foldl' (<>) mempty . intersperse "," $ map targetBuilder t

fromBuilder :: From -> Builder
fromBuilder (FromShort t) = textBuilder t
fromBuilder (FromFull db ret m) = 
  textBuilder db <> byteString ".\"" <> textBuilder ret <> "\"." <> textBuilder m

whereBuilder :: WhereClause -> Builder
whereBuilder (Eq a b) =       textBuilder a <> " = '"  <> textBuilder b <> "'"
whereBuilder (Gt a b) =       textBuilder a <> " > '"  <> textBuilder b <> "'"
whereBuilder (Lt a b) =       textBuilder a <> " < '"  <> textBuilder b <> "'"
whereBuilder (Ne a b) =       textBuilder a <> " != '" <> textBuilder b <> "'"
whereBuilder (Match a b) =    textBuilder a <> " =~ '" <> textBuilder b <> "'"
whereBuilder (NotMatch a b) = textBuilder a <> " !~ '" <> textBuilder b <> "'"
whereBuilder (And a b) = "(" <> whereBuilder a <> ") AND (" <> whereBuilder b <> ")"
whereBuilder (Or a b) =  "(" <> whereBuilder a <> ") OR (" <> whereBuilder b <> ")"

now :: Text
now = "now()"
