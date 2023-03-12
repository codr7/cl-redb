(in-package redb)

(defclass col (def)
  ())

(defmethod col-clone ((self col) name)
  (make-instance (type-of self) :name name))

(defmethod col-to-sql (self val)
  (to-sql val))

(defmacro define-col-type (name data-type)
  `(progn
     (defclass ,name (col)
       ())
     
     (defun ,(syms! 'new- name) (name)
       (make-instance ',name :name name))
     
     (defmethod data-type ((self ,name))
       ,data-type)))

(define-col-type boolean-col "BOOLEAN")

(defmethod boolean-to-sql (val)
  (if val "t" "f"))

(defmethod col-to-sql ((self boolean-col) val)
  (boolean-to-sql val))

(defun boolean-from-sql (val)
  (string= val "t"))

(defmethod col-from-sql ((self boolean-col) val)
  (boolean-from-sql val))

(define-col-type integer-col "INTEGER")

(defmethod to-sql ((self integer))
  (format nil "~a" self))

(defun integer-from-sql (val)
  (parse-integer val))

(defmethod col-from-sql ((self integer-col) val)
  (integer-from-sql val))

(define-col-type string-col "TEXT")

(defmethod col-to-sql ((self string-col) val)
  val)

(defmethod col-from-sql ((self string-col) val)
  val)

(define-col-type timestamp-col "TIMESTAMP")

(defmethod to-sql ((self timestamp))
  (format-timestring nil self)) 

(defun timestamp-from-sql (val)
  (flet ((p (i) (parse-integer val :start i :junk-allowed t)))
    (let* ((year (p 0))
	   (month (p 5))
	   (day (p 8))
	   (h (p 11))
	   (m (p 14))
	   (s (p 17)))
      (encode-timestamp 0 s m h day month year))))

(defmethod col-from-sql ((self timestamp-col) val)
  (timestamp-from-sql val))
