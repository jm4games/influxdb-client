{-# LANGUAGE OverloadedStrings #-}

module Database.InfluxDb.Query where

import Data.ByteString (ByteString)
import Data.ByteString.Builder (Builder, byteString, toLazyByteString)
import Data.ByteString.Lazy (toStrict)
import Data.List (foldl', intersperse)
import Data.Monoid ((<>))
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8)

import TextShow (TextShow, showt)

data SelectStatement = Select !Target !From
                     | SelectConditional Target !From !WhereClause

data Target = Count  !Text
            | Field  !Text
            | Targets ![Target]
            | Max    !Text
            | Mean   !Text
            | Min    !Text
            | Sum    !Text

data From = FromShort Text
          | FromFull Text Text Text

data WhereClause = And !WhereClause !WhereClause
                 | Or  !WhereClause !WhereClause
                 | Eq  !Text !Text
                 | Ne  !Text !Text 
                 | Gt  !Text !Text
                 | Lt  !Text !Text 

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

select :: Target -> From -> SelectStatement
select = Select

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
where' (Select t f) w = SelectConditional t f w
where' (SelectConditional t f w) w' = SelectConditional t f (And w w')

toByteString :: SelectStatement -> ByteString
toByteString (Select t f) = toStrict . toLazyByteString $ baseSelectBuilder t f
toByteString (SelectConditional t f w) = toStrict . toLazyByteString $ 
  baseSelectBuilder t f <> whereBuilder w

baseSelectBuilder :: Target -> From -> Builder
baseSelectBuilder t f = byteString "SELECT " <> targetBuilder t <> byteString " " <> fromBuilder f

textBuilder :: Text -> Builder
textBuilder = byteString . encodeUtf8

targetBuilder :: Target -> Builder
targetBuilder (Field f) = textBuilder f 
targetBuilder (Count f) = byteString "COUNT(" <> textBuilder f <> byteString ")"
targetBuilder (Max f) = byteString "MAX(" <> textBuilder f <> byteString ")"
targetBuilder (Min f) = byteString "MIN(" <> textBuilder f <> byteString ")"
targetBuilder (Mean f) = byteString "MEAN(" <> textBuilder f <> byteString ")"
targetBuilder (Sum f) = byteString "SUM(" <> textBuilder f <> byteString ")"
targetBuilder (Targets t) = foldl' (<>) mempty . intersperse (byteString ",") $ map targetBuilder t

fromBuilder :: From -> Builder
fromBuilder (FromShort t) = textBuilder t
fromBuilder (FromFull db ret m) = 
  textBuilder db <> byteString ".\"" <> textBuilder ret <> "\"." <> textBuilder m

whereBuilder :: WhereClause -> Builder
whereBuilder (Eq a b) = textBuilder a <> byteString "='" <> textBuilder b <> byteString "'"
whereBuilder (Gt a b) = textBuilder a <> byteString ">'" <> textBuilder b <> byteString "'"
whereBuilder (Lt a b) = textBuilder a <> byteString "<'" <> textBuilder b <> byteString "'"

temp :: SelectStatement
temp = select (maxT "*") (from "test") `where'` "temp" .>. (5::Int) .&&. "temp" .<. (10::Int)

