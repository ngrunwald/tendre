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
              @(assoc! ttm :foo 0) =in=> {:foo 0})
        (dotimes [n 1000]
          (update! ttm :foo inc))
        (fact "all written"
              (ttm :foo) => 1000)
        (fact "isolation preserved"
              (tm :foo) => nil?))
      -(fact "changes are persisted"
             (tm :foo) => 1000)
      (fact "closing works"
            (close! tm) => any)
      (finally
        (tail-recursive-delete path)))))

(deftest multi-transactions
  (let [path (tmp-path)
        tm1 (make-map path {:name "tm1"})
        tm2 (make-map tm1 {:name "tm2"})]
    (try
      (with-transaction [ttm1 tm1
                         ttm2 tm2]
        (facts "empty map"
               (into {} ttm1) => {}
               (empty? ttm2) => true)
        (facts "put 0 and 1"
               @(assoc! ttm1 :foo 0) =in=> {:foo 0}
               @(assoc! ttm2 :bar (inc (:foo ttm1))) =in=> {:bar 1})
        (facts "isolation preserved"
               (tm1 :foo) => nil?
               (tm2 :bar) => nil?)
        (abort! ttm2))
      -(facts "changes are not persisted"
              (tm1 :foo) => nil?
              (tm2 :bar) => nil?)
      (facts "closing works"
             (close! tm1) => any
             (close! tm1) => any)
      (finally
        (tail-recursive-delete path)))))
