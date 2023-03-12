(in-package redb)

(defclass col (def)
  ((null? :initarg :null? :initform t :reader null?)))

(defmethod col-clone ((col col) name)
  (make-instance (type-of col) :name name))

(defmethod col-to-sql (col val)
  (to-sql val))

(defmacro define-col-type (name data-type)
  `(progn
     (defclass ,name (col)
       ())
     
     (defun ,(sym 'new- name) (name)
       (make-instance ',name :name name))
     
     (defmethod data-type ((col ,name))
       ,data-type)))

(define-col-type boolean-col "BOOLEAN")

(defmethod boolean-to-sql (val)
  (if val "t" "f"))

(defmethod col-to-sql ((col boolean-col) val)
  (boolean-to-sql val))

(defun boolean-from-sql (val)
  (string= val "t"))

(defmethod col-from-sql ((col boolean-col) val)
  (boolean-from-sql val))

(define-col-type integer-col "INTEGER")

(defmethod to-sql ((col integer))
  (format nil "~a" col))

(defun integer-from-sql (val)
  (parse-integer val))

(defmethod col-from-sql ((col integer-col) val)
  (integer-from-sql val))

(define-col-type string-col "TEXT")

(defmethod col-to-sql ((col string-col) val)
  val)

(defmethod col-from-sql ((col string-col) val)
  val)

(define-col-type timestamp-col "TIMESTAMP")

(defmethod to-sql ((col timestamp))
  (format-timestring nil col)) 

(defun timestamp-from-sql (val)
  (flet ((p (i) (parse-integer val :start i :junk-allowed t)))
    (let* ((year (p 0))
	   (month (p 5))
	   (day (p 8))
	   (h (p 11))
	   (m (p 14))
	   (s (p 17)))
      (encode-timestamp 0 s m h day month year))))

(defmethod col-from-sql ((col timestamp-col) val)
  (timestamp-from-sql val))
