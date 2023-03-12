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

(defun test-rec ()
  (let* ((tbl (new-table :foo :bar))
	 (col (new-integer-col tbl :bar))
	 (rec (new-rec)))
    (assert (null (field rec col)))
    (setf (field rec col) 42)
    (assert (= 42 (field rec col)))))
