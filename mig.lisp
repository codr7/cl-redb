(in-package redb)

(defvar *mig*)

(defstruct mig
  (id (error "Missing id") :type integer)
  (notes (error "Missing notes") :type string)
  (up (error "Missing up") :type function)
  (down (error "Missing down") :type function))

(defun new-mig (notes up down)
  (make-mig :id (next-val (db mig-id)) :notes notes :up up :down down))

(defun push-mig (notes up down)
  (let ((m (new-mig notes up down)))
    (push m *mig*)
    m))

(defmethod up (mig)
  (with-tx ()
    (funcall (mig-up mig))
    
    (let ((rec (new-rec (db mig id) (mig-id mig)
			(db mig at) (now)
			(db mig notes) (mig-notes mig))))
      (store-rec (db mig) rec))))

(defmethod down (mig)
  (with-tx ()
    (funcall (mig-down mig))
    
    (let ((rec (new-rec (db mig id) (mig-id mig))))
      (delete-rec (db mig) rec))))

(defun mig (&key id)
  (let ((result 0))
    (dolist (m (reverse *mig*))
      (cond
	((or (null id) (<= (mig-id m) id))
	 (unless (rec-exists? (db mig) (mig-id m))
	   (up m)
	   (incf result)))
	
	((and id (> (mig-id m) id))
	 (when (rec-exists? (db mig) (mig-id m))
	   (down m)
	   (incf result)))))
    
    result))
