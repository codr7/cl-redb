(in-package redb)

(defvar *tx* nil)

(defstruct tx
  (cx nil :type (or cx null))
  (prev nil :type (or tx null))
  (save-point nil :type (or null string))
  (stored-vals (make-hash-table) :type hash-table)
  (active? t :type boolean))

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
	(send-dml (format nil "SAVEPOINT ~a" sp) nil))
      
      (send-dml "BEGIN" nil)))

(defun commit (&key (tx *tx*))
  (unless (tx-active? tx)
    (error "Attempt to commit inactive transaction"))
  
  (dohash (f v (tx-stored-vals tx))
    (if (tx-prev tx)
	(setf (tx-val f :tx (tx-prev tx)) v)
	(setf (cx-val f :cx (tx-cx tx)) v)))
  
  (unless (tx-prev tx)
    (send-dml "COMMIT" nil))
  
  (setf (tx-active? tx) nil))

(defun rollback (&key (tx *tx*))
  (unless (tx-active? tx)
    (error "Attempt to rollback inactive transaction"))
  
  (let ((sp (tx-save-point tx)))
    (if sp
	(send-dml (format nil "ROLLBACK TO ~a" sp) nil)
	(send-dml "ROLLBACK" nil)))
  
  (setf (tx-active? tx) nil))

(defmacro with-tx ((&key cx prev) &body body)
  `(let ((*tx* (new-tx (or ,cx *cx*) (or ,prev *tx*))))
     (begin)
     
     (unwind-protect
	  (progn
	    ,@body
	    (when (tx-active? *tx*)
	      (commit)))
       (when (tx-active? *tx*)
	 (rollback)))))
