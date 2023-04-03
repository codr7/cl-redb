(in-package redb)

(defvar *migs* nil)

(defstruct mig
  (id (error "Missing id") :type integer)
  (notes (error "Missing notes") :type string)
  (up (error "Missing up") :type function)
  (down (error "Missing down") :type function))

(defun new-mig (id notes up down)
  (make-mig :id id :notes notes :up up :down down))

(defmethod up (mig)
  (with-tx ()
    (funcall (mig-up mig))
    
    (let ((rec (new-rec (db migs id) (mig-id mig)
			(db migs at) (now)
			(db migs notes) (mig-notes mig))))
      (store-rec (db migs) rec))))

(defmethod down (mig)
  (with-tx ()
    (funcall (mig-down mig))
    
    (let ((rec (new-rec (db migs id) (mig-id mig))))
      (delete-rec (db migs) rec))))

  
