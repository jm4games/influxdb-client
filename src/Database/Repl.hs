module Database.InfluxDb.Repl(
    module IDb
  , T(..)
  , I(..)
  ) where

import Data.Text(Text)

import Database.InfluxDb as IDb
import Database.InfluxDb.Query as IDb

import TextShow(TextShow(..))

newtype T = T Text

instance TextShow T where
  showbPrec prec (T t) = showbPrec prec t
  showb (T t) = showb t

newtype I = I Int

instance TextShow I where
  showbPrec prec (I i) = showbPrec prec i
  showb (I i) = showb i
