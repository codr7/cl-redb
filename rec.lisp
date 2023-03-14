(in-package redb)

(defstruct field
  (col nil :type (or col null))
  (val nil :type t))

(defstruct rec
  (fields nil :type list))

(defun new-rec ()
  (make-rec))

(defun find-field (rec col)
  (first (member col (rec-fields rec) :test (lambda (x y) (eq x (field-col y))))))

(defun field (rec col)
  (let ((found (find-field rec col)))
    (when found (field-val found))))

(defun (setf field) (val rec col)
  (let ((found (or (find-field rec col))))
    (if found
	(setf (field-val found) val)
	(push (make-field :col col :val val) (rec-fields rec)))))

(defun store-field (fld)
  (if *tx*
      (setf (tx-val fld) (field-val fld))
      (setf (cx-val fld) (field-val fld))))

(defun stored-val (fld)
  (or (and *tx* (tx-val fld)) (cx-val fld)))

(defun stored? (rec &rest cols)
  (member-if (lambda (c)
	       (let ((f (find-field rec c)))
		 (and f (stored-val f))))
	     cols))

(defun load-rec (rec cols result &key (col 0) (row 0))
  (dolist (c cols)
    (setf (field rec c) (PQgetvalue result row col))
    (incf col))
  rec)

(defun store-rec (rec tbl)
  (if (apply #'stored? rec (cols tbl))
      (update-rec tbl rec)
      (insert-rec tbl rec))

  (dolist (f (rec-fields rec))
    (store-field f)))

(defun test-rec ()
  (with-db (test-db)
    (let ((rec (new-rec)))
      (assert (null (field rec (db users alias))))
      (setf (field rec (db users alias)) "foo")
      (assert (string= (field rec (db users alias)) "foo"))
      
      (with-cx ("test" "test" "test")	
	(drop *db*)
	(create *db*)
	
	(assert (not (apply #'stored? rec (cols (db users)))))

	(with-tx ()
	  (setf (field rec (db users alias)) "bar")
	  (store rec (db users))
	  (assert (apply #'stored? rec (cols (db users))))

	  (with-tx ()
	    (setf (field rec (db users alias)) "baz")
	    (store rec (db users))
	    (rollback))

	  (assert (apply #'stored? rec (cols (db users))))

	  (do-result (res (find-rec (db users) "bar"))
	    (load-rec rec (cols (db users)) res))
	  
	  (assert (string= (field rec (db users alias)) "bar")))))))

