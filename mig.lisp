(in-package redb)

(defvar *mig*)

(defparameter *mig-db*
  '(table mig (id)
    (col id id)
    (col at tstamp)))

(defstruct mig
  (id (error "Missing id") :type integer)
  (up (error "Missing up") :type function)
  (down (error "Missing down") :type function))

(defun new-mig (id up down)
  (make-mig :id id :up up :down down))

(defun push-mig (id up down)
  (let ((m (new-mig id up down)))
    (push m *mig*)
    m))

(defmethod run-mig-up (mig)
  (with-tx ()
    (funcall (mig-up mig))
    
    (let ((rec (new-rec (db mig id) (mig-id mig)
			(db mig at) (now))))
      (store-rec (db mig) rec))))

(defmethod run-mig-down (mig)
  (with-tx ()
    (funcall (mig-down mig))
    
    (let ((rec (new-rec (db mig id) (mig-id mig))))
      (delete-rec (db mig) rec))))

(defun run-mig (&key id)
  (let ((result 0))
    (dolist (m (reverse *mig*))
      (cond
	((or (null id) (<= (mig-id m) id))
	 (unless (rec-exists? (db mig) (mig-id m))
	   (run-mig-up m)
	   (incf result)))
	
	((and id (> (mig-id m) id))
	 (when (rec-exists? (db mig) (mig-id m))
	   (run-mig-down m)
	   (incf result)))))
    
    result))
