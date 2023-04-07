(in-package redb)

(defmethod sql-name ((in string))
  (with-output-to-string (out)
    (dotimes (i (len in))
      (let ((c (char in i)))
	(write-char (if (char= c #\-) #\_ c) out)))))

(defmethod sql-name ((in symbol))
  (sql-name (string-downcase (symbol-name in))))

(defmethod sql ((x string))
  x)

(defmethod params ((x string)))
