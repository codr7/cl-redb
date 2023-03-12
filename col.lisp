(in-package redb)

(defclass col (table-def)
  ((null? :initarg :null? :initform t :reader null?)))

(defmethod cols ((col col))
  `#(,col))

(defmethod col-clone ((col col) name)
  (make-instance (type-of col) :name name))

(defmacro define-col-type (name data-type)
  `(progn
     (defclass ,name (col)
       ())
     
     (defun ,(sym 'new- name) (tbl name)
       (make-instance ',name :table tbl :name name))
     
     (defmethod data-type ((col ,name))
       ,data-type)))

(define-col-type boolean-col "BOOLEAN")

(defmethod boolean-to-sql (val)
  (if val "t" "f"))

(defmethod to-sql ((col boolean-col) val)
  (boolean-to-sql val))

(defun boolean-from-sql (val)
  (string= val "t"))

(defmethod from-sql ((col boolean-col) val)
  (boolean-from-sql val))

(define-col-type integer-col "INTEGER")

(defun integer-to-sql (val)
  (format nil "~a" val))
  
(defmethod to-sql ((col integer-col) val)
  (integer-to-sql val))

(defun integer-from-sql (val)
  (parse-integer val))

(defmethod from-sql ((col integer-col) val)
  (integer-from-sql val))

(define-col-type string-col "TEXT")

(defmethod to-sql ((col string-col) val)
  val)

(defmethod from-sql ((col string-col) val)
  val)

(define-col-type timestamp-col "TIMESTAMP")

(defmethod to-sql ((col timestamp-col) val)
  (format-timestring nil val)) 

(defun timestamp-from-sql (val)
  (flet ((p (i) (parse-integer val :start i :junk-allowed t)))
    (let* ((year (p 0))
	   (month (p 5))
	   (day (p 8))
	   (h (p 11))
	   (m (p 14))
	   (s (p 17)))
      (encode-timestamp 0 s m h day month year))))

(defmethod from-sql ((col timestamp-col) val)
  (timestamp-from-sql val))
