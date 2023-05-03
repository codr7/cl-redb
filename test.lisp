(defpackage redb-test
  (:use cl redb)
  (:export run))

(in-package redb-test)

(define-db test-db ()
  (table users (alias)
	 (col alias text)
	 (col name1 text :null? t)
	 (col name2 text :null? t))
  
  (table admins (who-alias)
	 (fkey who users)))

(defun test-db ()
  (assert (= (len (cols (db users))) 3))
  (assert (= (len (cols (pkey (db users)))) 1))
  (assert (= (len (cols (db admins who))) 1)))

(defun test-enum ()
  (assert (add-enum (db event-type) :foo))
  (assert (create (db event-type)))
  (assert (remove-enum (db event-type) :foo)))
  ;(assert (create (db event-type)))

(define-event user-insert (e)
  )

(define-event user-update (e)
  )

(defun test-events ()
  )

(defun test-migration ()
  (let ((rec (new-rec (db users alias) "foo")))
    (push-migration 1
	      (lambda ()
		(store-rec (db users) rec))
	      (lambda ()
		(delete-rec (db users) rec)))

    (push-migration 2
	      (lambda ()
		(setf (field rec (db users alias)) "bar")
		(store-rec (db users) rec))
	      (lambda ()
		(setf (field rec (db users alias)) "foo")
		(store-rec (db users) rec))))
  
  (assert (= (migrate :id 1) 1))
  (assert (rec-exists? (db users) "foo"))
  (assert (not (rec-exists? (db users) "bar")))
  
  (assert (= (migrate) 1))
  (assert (rec-exists? (db users) "bar"))
  (assert (not (rec-exists? (db users) "foo")))
  (assert (= (migrate) 0))

  (assert (= (migrate :id 1) 1))
  (assert (rec-exists? (db users) "foo"))
  (assert (not (rec-exists? (db users) "bar")))
  
  (assert (= (migrate :id 0) 1))
  (assert (not (rec-exists? (db users) "foo")))
  (assert (not (rec-exists? (db users) "bar"))))

(defun test-query ()
  (let* ((usr (new-rec (db users alias) "foo"))
	 (adm (new-rec (db admins who) usr))
	 (q (new-query)))
    (store-rec (db users) usr)
    (store-rec (db admins) adm)
    (select q (db admins) (db users))
    (prepare q)
    
    (with-query (q)
      (let ((rec (next)))
	(assert (rec= usr rec))
	(assert (rec= adm rec)))
      (assert (not (next))))))

(defun test-rec ()
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
	 (adm (new-rec (db admins who) usr)))
    (assert (rec= usr (field adm (db admins who))))))

(defun test-seq ()
  (let ((v (next-val (db event-id))))
    (assert (= (next-val (db event-id)) (+ v 1)))))

(defun test-table ()
  (let* ((tbl (new-table :foo :bar))
	 (col (new-integer-col tbl :bar)))
    (assert (not (exists? tbl)))
    (create tbl)
    (assert (exists? tbl))
    (assert (exists? col))
    (drop tbl)
    (assert (not (exists? tbl)))))

(defun test-tx ()
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
    (assert (not (modified? rec (db users alias))))))

(defun run-test (fn)
  (with-db (test-db)
    (with-cx ("test" "test" "test")
      (with-tx ()
	(funcall fn)
	(rollback)))))

(defun init-db ()
  (with-db (test-db)
    (with-cx ("test" "test" "test")
      (drop *db*)
      (create *db*))))

(defun run ()
  (init-db)
  
  (let ((tests (list #'test-db
		     #'test-enum
		     #'test-events
		     #'test-migration
		     #'test-query
		     #'test-rec
		     #'test-seq
		     #'test-table
		     #'test-tx)))
    (dolist (f tests)
      (funcall #'run-test f))))
