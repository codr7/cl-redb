(in-package redb)

(defvar *tx* nil)

(defstruct tx
  (cx nil :type (or cx null))
  (prev nil :type (or tx null))
  (save-point nil :type (or null string))
  (stored-vals (make-hash-table) :type hash-table))

(defun new-tx (cx prev)
  (make-tx :cx cx :prev prev))

(defun tx-val (fld &key (tx *tx*))
  (gethash fld (tx-stored-vals tx)))

(defun (setf tx-val) (val fld &key (tx *tx*))
  (sethash fld (tx-stored-vals tx) val))

(defun begin (&key (tx *tx*))
  (if (tx-prev tx)
      (let ((sp (sql-name (gensym))))
	(setf (tx-save-point tx) sp)
	(send-command (format nil "SAVEPOINT ~a" sp) nil))
      
      (send-command "BEGIN" nil)))

(defun commit (&key (tx *tx*))
  (dohash (f v (tx-stored-vals tx))
    (if (tx-prev tx)
	(setf (tx-val f :tx (tx-prev tx)) v)
	(progn
	  (setf (cx-val f :cx (tx-cx tx)) v)
	  (send-command "COMMIT" nil)))))

(defun rollback (&key (tx *tx*))
  (let ((sp (tx-save-point tx)))
    (if sp
	(send-command (format nil "ROLLBACK TO ~a" sp) nil)
	(send-command "ROLLBACK" nil)))
  
  (clrhash (tx-stored-vals tx)))

(defmacro with-tx ((&key cx prev) &body body)
  `(let ((*tx* (new-tx (or ,cx *cx*) (or ,prev *tx*))))
     (begin)
     
     (unwind-protect
	  (progn
	    ,@body
	    (commit))
       (rollback))))
