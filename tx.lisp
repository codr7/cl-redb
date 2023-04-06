(in-package redb)

(defvar *tx* nil)

(defstruct tx
  (prev nil :type (or tx null))
  (active? t :type boolean)
  (save-point nil :type (or null string))
  (stored-vals (make-hash-table) :type hash-table))

(defun new-tx (prev)
  (make-tx :prev prev))

(defmethod print-object ((tx tx) out)
  (format out "(tx ~a ~a ~a)" (tx-prev tx) (tx-active? tx) (len (tx-stored-vals tx))))

(defun tx-val (fld &key (tx *tx*))
  (or (gethash fld (tx-stored-vals tx))
      (and (tx-prev tx) (tx-val fld :tx (tx-prev tx)))
      (cx-val fld)))

(defun (setf tx-val) (val fld &key (tx *tx*))
  (sethash fld (tx-stored-vals tx) val))

(defun begin (&key (tx *tx*))
  (if (tx-prev tx)
      (let ((sp (sql-name (gensym))))
	(setf (tx-save-point tx) sp)
	(send-cmd (format nil "SAVEPOINT ~a" sp) nil))
      
      (send-cmd "BEGIN" nil)))

(defun commit (&key (tx *tx*))
  (unless (tx-active? tx)
    (error "Attempt to commit inactive transaction"))
  
  (dohash (f v (tx-stored-vals tx))
    (if (tx-prev tx)
	(setf (tx-val f :tx (tx-prev tx)) v)
	(setf (cx-val f) v)))
  
  (unless (tx-prev tx)
    (send-cmd "COMMIT" nil))
  
  (setf (tx-active? tx) nil))

(defun rollback (&key (tx *tx*))
  (unless (tx-active? tx)
    (error "Attempt to rollback inactive transaction"))
  
  (let ((sp (tx-save-point tx)))
    (if sp
	(send-cmd (format nil "ROLLBACK TO ~a" sp) nil)
	(send-cmd "ROLLBACK" nil)))
  
  (setf (tx-active? tx) nil))

(defmacro with-tx ((&key tx) &body body)
  `(let ((*tx* (or ,tx (new-tx *tx*))))
     (begin)
     
     (unwind-protect
	  (progn
	    ,@body
	    (when (tx-active? *tx*)
	      (commit)))
       (when (tx-active? *tx*)
	 (rollback)))))
