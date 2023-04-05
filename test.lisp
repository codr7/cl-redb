(defpackage redb-test
  (:use cl redb)
  (:export run))

(in-package redb-test)

(define-db test-db
  (seq mig-id)
  (table mig (id)
	 (col id bigint)
	 (col at tstamp)
	 (col notes text))
  (table users (alias)
	 (col alias text)
	 (col name1 text :null? t)
	 (col name2 text :null? t))
  (seq event-id)
  (enum event-type
	user-created
	user-updated)
  (table events (id)
	 (col id bigint)
	 (col type event-type)
	 (col meta json :null? t)
	 (col body json :null? t)
	 (col at tstamp)
	 (fkey by users)
	 (index at-idx nil at)))

(defun test-cx ()
  (with-cx ("test" "test" "test")
    (send "SELECT * FROM pg_tables" '())
    
    (multiple-value-bind (result status) (recv)
      (assert (eq status :PGRES_TUPLES_OK))
      (PQclear result)
      (assert (null (recv))))))

(defun test-db ()
  (with-db (test-db)
    (assert (= (length (cols (db users))) 3))
    (assert (= (length (cols (pkey (db users)))) 1))
    (assert (= (length (cols (db events by))) 1))
    
    (with-cx ("test" "test" "test")
      (drop *db*)
      (create *db*))))

(defun test-mig ()
  (with-db (test-db)
    (with-cx ("test" "test" "test")
      (drop *db*)
      (create *db*)

      (let* ((a "foo")
	     (rec (new-rec (db users alias) a)))
	(push-mig "create user"
		 (lambda ()
		   (store-rec (db users) rec))
		 (lambda ()
		   (delete-rec (db users) rec)))
	(assert (= (mig) 1))
	(assert (rec-exists? (db users) a))
	(assert (= (mig) 0))
	
	(assert (= (mig :id 0) 1))
	(assert (not (rec-exists? (db users) a)))))))

(defun test-query ()
  (with-db (test-db)    
    (with-cx ("test" "test" "test")
      (drop *db*)
      (create *db*)

      (let* ((usr (new-rec (db users alias) "foo"))
	     (evt (new-rec (db events id) (next-val (db event-id))
			   (db events by) usr
			   (db events type) :user-created
			   (db events at) (now)))
	     (q (new-query)))
	(store-rec (db users) usr)
	(store-rec (db events) evt)
	(select q (db events) (db users))
	(prepare q)
	
	(with-query (q)
	  (let ((rec (next)))
	    (assert (rec= usr rec))
	    (assert (rec= evt rec)))
	  (assert (not (next))))))))

(defun test-rec ()
  (with-db (test-db)
    (let ((rec (new-rec)))
      (assert (null (field rec (db users alias))))
      (setf (field rec (db users alias)) "foo")
      (assert (string= (field rec (db users alias)) "foo")))

    (let ((rec (new-rec (db users alias) "foo"
			(db users name1) "Foo"
			(db users name2) "Bar")))
      (assert (string= (field rec (db users alias)) "foo"))
      (assert (string= (field rec (db users name1)) "Foo"))
      (assert (string= (field rec (db users name2)) "Bar")))

    (let* ((usr (new-rec (db users alias) "foo"))
	   (evt (new-rec (db events by) usr)))
      (assert (rec= usr (field evt (db events by)))))
    
    (with-cx ("test" "test" "test")	
      (drop *db*)
      (create *db*)

      (let ((rec (new-rec (db users alias) "foo")))
	(assert (not (stored? rec (db users alias))))
	(assert (modified? rec (db users alias)))
	
	(with-tx ()
	  (setf (field rec (db users alias)) "bar")
	  (store-rec (db users) rec))

	(assert (stored? rec (db users alias)))
	(assert (not (modified? rec (db users alias))))
	
	(with-tx ()
	  (setf (field rec (db users alias)) "baz")
	  (store-rec (db users) rec)
	  (rollback))

	(assert (stored? rec (db users alias)))
	(assert (modified? rec (db users alias)))

	(with-result (res (find-rec (db users) "bar"))
	  (load-rec rec (cols (db users)) res))
	
	(assert (string= (field rec (db users alias)) "bar"))
	(assert (stored? rec (db users alias)))
	(assert (not (modified? rec (db users alias))))))))

(defun test-seq ()
  (with-db (test-db)
    (with-cx ("test" "test" "test")
      (drop *db*)
      (create *db*)
      
      (assert (= (next-val (db event-id)) 1))
      (assert (= (next-val (db event-id)) 2)))))

(defun test-table ()
  (with-cx ("test" "test" "test")
    (let* ((tbl (new-table :foo :bar))
	   (col (new-integer-col tbl :bar)))
      (assert (not (exists? tbl)))
      (create tbl)
      (assert (exists? tbl))
      (assert (exists? col))
      (drop tbl)
      (assert (not (exists? tbl))))))

(defun run ()
  (test-cx)
  (test-db)
  (test-mig)
  (test-query)
  (test-rec)
  (test-seq)
  (test-table))
