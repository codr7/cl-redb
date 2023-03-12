(in-package redb)

(defmacro dohash ((k v tbl) &body body)
  (let (($i (gensym)) ($k (gensym)) ($next (gensym)) ($ok (gensym)))
    `(with-hash-table-iterator (,$i ,tbl)
       (tagbody
	  ,$next
	  (multiple-value-bind (,$ok ,$k ,v) (,$i)
	    (declare (ignorable ,v))
	    (when ,$ok
	      (let ((,k ,$k))
		(declare (ignorable ,k))
		,@body
		(go ,$next))))))))

(defmacro let-when ((var form) &body body)
  `(let ((,var ,form))
     (when ,var
       ,@body)))

(defmethod str! ((val string))
  val)

(defmethod str! ((val symbol))
  (string-downcase (symbol-name val)))

(defun syms (&rest args)
  (with-output-to-string (out)
    (dolist (a args)
      (etypecase a
	(symbol (princ a out))
	(string (princ (string-upcase a) out))))))

(defun sym (&rest args)
  (intern (apply #'syms args)))

(defun kw (&rest args)
  (intern (apply #'syms args) 'keyword))
