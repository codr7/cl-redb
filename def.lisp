(in-package redb)

(defclass def ()
  ((name :initarg :name :initform (error "missing name") :reader name)))

(defmethod to-sql ((self def))
  (to-sql (name self)))

(defclass rel ()
  ((cols :initform (make-array 0 :element-type 'col :fill-pointer 0) :reader cols)
   (col-lookup :initform (make-hash-table))))

(defun add-col (rel col)
  (with-slots (cols col-lookup) rel
    (setf (gethash (name col) col-lookup) (length cols))
    (vector-push-extend col cols)))

(defmacro do-cols ((col rel) &body body)
  (let* ((i (gensym)))
    `(dotimes (,i (length (cols ,rel)))
       (let* ((,col (aref (cols ,rel) ,i)))
	 ,@body))))
