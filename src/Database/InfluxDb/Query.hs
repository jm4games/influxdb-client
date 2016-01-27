{-# LANGUAGE OverloadedStrings #-}

module Database.InfluxDb.Query where

import Data.Text (Text)

import TextShow (TextShow, showt)

data SelectStatement = Select !Target !From
                     | SelectConditional Target !From !WhereClause

data Target = Count !Text
            | Field !Text
            | Max   !Text
            | Mean  !Text
            | Min   !Text
            | Sum   !Text

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

temp :: SelectStatement
temp = select (maxT "*") (from "test") `where'` "temp" .>. (5::Int) .&&. "temp" .<. (10::Int)

