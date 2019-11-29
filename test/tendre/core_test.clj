(ns tendre.core-test
  (:require [clojure.test :refer :all]
            [tendre.core :refer :all]
            [testit.core :refer :all])
  (:import [jetbrains.exodus.env Transaction]))

(defn tmp-path
  []
  (str (System/getProperty "java.io.tmpdir") "/" (name (gensym "tendre-test-"))))

(defn tail-recursive-delete
  [& fs]
  (when-let [f (first fs)]
    (if-let [cs (seq (.listFiles (clojure.java.io/file f)))]
      (recur (concat cs fs))
      (do (clojure.java.io/delete-file f)
          (recur (rest fs))))))

(deftest isolation
  (let [path (tmp-path)]
    (try
      (with-open [tm (make-map path {})]
        (fact "empty map"
             (into {} tm) => {})
        (assoc! tm :foo 42)
        (facts "assoc"
               (tm :foo) => 42
               (:foo tm) => 42
               (get-transaction-type tm) => nil?)
        (let [finished? (future
                          (dotimes [n 1000]
                            (update! tm :foo inc)))]
          (dotimes [n 1000]
            (update! tm :foo inc))
          @finished?
          (fact "transactions reconciled"
                (tm :foo) => 2042)))
      (finally
        (tail-recursive-delete path)))))

(deftest transactions
  (let [path (tmp-path)
        tm (make-map path {})]
    (try
      (with-transaction [ttm tm]
        (fact "empty map"
              (into {} ttm) => {})
        (fact "put 0"
              (assoc! ttm :foo 0) => #(= (:foo %) 0))
        (dotimes [n 1000]
          (update! ttm :foo inc))
        (fact "all written"
              (ttm :foo) => 1000)
        (fact "isolation preserved"
              (tm :foo) => nil?))
      (fact "closing works"
            (close! tm) => (fn [_] true))
      (finally
        (tail-recursive-delete path)))))
