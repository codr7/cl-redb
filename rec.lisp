(in-package redb)

(defstruct field
  (col nil :type (or col null))
  (val nil :type t))

(defstruct rec
  (fields nil :type list))

(defun new-rec (&rest fields)
  (let ((rec (make-rec)))
    (labels ((set-fields (in)
	       (when in
		   (setf (field rec (pop in)) (pop in))
		   (set-fields in))))
      (set-fields fields))
    rec))

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

(defun stored-val (fld)
  (or (and *tx* (tx-val fld)) (cx-val fld)))

(defun (setf stored-val) (val fld)
  (if *tx*
      (setf (tx-val fld) val)
      (setf (cx-val fld) val)))

(defun store-field (fld)
  (setf (stored-val fld) (field-val fld)))

(defun stored? (rec &rest cols)
  (member-if (lambda (c)
	       (let ((f (find-field rec c)))
		 (and f (stored-val f))))
	     cols))

(defun modified? (rec &rest cols)
  (member-if (lambda (c)
	       (let ((f (find-field rec c)))
		 (not (eq (field-val f) (stored-val f)))))
	     cols))

(defun load-rec (rec cols result &key (col 0) (row 0))
  (dolist (c cols)
    (let ((v (from-sql c (PQgetvalue result row col))))
      (setf (field rec c) v)
      (setf (stored-val (find-field rec c)) v
	    ))
    (incf col))
  rec)

(defun store-rec (rec tbl)
  (if (apply #'stored? rec (cols tbl))
      (update-rec tbl rec)
      (insert-rec tbl rec))

  (dolist (f (rec-fields rec))
    (store-field f)))

(defun rec= (x y)
  (dolist (f (rec-fields x))
    (unless (col= (field-col f) (field-val f) (field y (field-col f)))
      (return-from rec=)))
  t)
      
-
