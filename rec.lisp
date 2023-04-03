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

(defmethod field ((rec rec) (col col))
  (let ((found (find-field rec col)))
    (when found (field-val found))))

(defmethod field ((rec rec) (key fkey))
  (let ((frec (new-rec)))
    (dolist (c (foreign-cols key))
      (setf (field frec (rest c)) (field rec (first c))))
    frec))

(defmethod set-field ((rec rec) (col col) val)
  (let ((found (or (find-field rec col))))
    (if found
	(setf (field-val found) val)
	(push (make-field :col col :val val) (rec-fields rec)))))

(defmethod set-field ((rec rec) (key fkey) (frec rec))
  (dolist (c (foreign-cols key))
    (set-field rec (first c) (field frec (rest c)))))

(defun (setf field) (val rec col)
  (set-field rec col val))

(defun stored-val (fld)
  (or (and *tx* (tx-val fld)) (cx-val fld)))

(defun (setf stored-val) (val fld)
  (if *tx*
      (setf (tx-val fld) val)
      (setf (cx-val fld) val)))

(defun store-field (fld)
  (setf (stored-val fld) (field-val fld)))

(defun stored-field (rec col)
  (let ((f (find-field rec col)))
    (or (stored-val (find-field rec col)) (field-val f))))

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
      (setf (stored-val (find-field rec c)) v))
    (incf col))
  rec)

(defun store-rec (tbl rec)
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
