(in-package redb)

(defmacro dohash ((k v tbl) &body body)
  (let* (($i (gensym)) ($k (gensym)) ($next (gensym)) ($ok (gensym)))
    `(with-hash-table-iterator (,$i ,tbl)
       (tagbody
	  ,$next
	  (multiple-value-bind (,$ok ,$k ,v) (,$i)
	    (declare (ignorable ,v))
	    (when ,$ok
	      (let* ((,k ,$k))
		(declare (ignorable ,k))
		,@body
		(go ,$next))))))))

(defmethod kw! ((val symbol))
  (kw! (symbol-name val)))

(defmethod kw! ((val string))
  (intern (string-upcase val) 'keyword))

(defmacro let-when ((var form) &body body)
  `(let ((,var ,form))
     (when ,var
       ,@body)))

(defmethod str! ((val string))
  val)

(defmethod str! ((val symbol))
  (string-downcase (symbol-name val)))

(defmethod sym! ((val symbol))
  val)

(defmethod sym! ((val string))
  (intern (string-upcase val)))

(defun syms! (&rest vals)
  (intern
   (with-output-to-string (out)
     (dolist (v vals)
       (princ v out)))))
