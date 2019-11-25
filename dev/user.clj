(ns user
  (:require [expound.alpha :as expound]
            [clojure.spec.alpha :as s]))

(set! s/*explain-out* expound/printer)
