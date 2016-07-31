module Database.InfluxDb.Repl where

import Data.Text(Text)

import TextShow(TextShow(..))

newtype T = T Text

instance TextShow T where
  showbPrec prec (T t) = showbPrec prec t
  showb (T t) = showb t

newtype I = I Int

instance TextShow I where
  showbPrec prec (I i) = showbPrec prec i
  showb (I i) = showb i
