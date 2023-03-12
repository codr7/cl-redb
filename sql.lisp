(in-package redb)

(defmethod to-sql ((self string))
  (let* ((out (copy-seq self)))
    (dotimes (i (length out))
      (let* ((c (char out i)))
	(when (char= c #\-)
	  (setf (char out i) #\_))))
    out))

(defmethod to-sql ((self symbol))
  (to-sql (string-downcase (symbol-name self))))
