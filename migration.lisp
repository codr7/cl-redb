(in-package redb)

(defvar *migrations*)

(defparameter *migration-db*
  '((table migrations (id)
     (col id bigint)
     (col at timestamp))))

(defstruct migration
  (id (error "Missing id") :type integer)
  (up (error "Missing up") :type function)
  (down (error "Missing down") :type function))

(defun new-migration (id up down)
  (make-migration :id id :up up :down down))

(defun push-migration (id up down)
  (let ((m (new-migration id up down)))
    (push m *migrations*)
    m))

(defmethod migrate-up (mig)
  (with-tx ()
    (funcall (migration-up mig))
    
    (let ((rec (new-rec (db migrations id) (migration-id mig)
			(db migrations at) (now))))
      (store-rec (db migrations) rec))))

(defmethod migrate-down (mig)
  (with-tx ()
    (funcall (migration-down mig))
    
    (let ((rec (new-rec (db migrations id) (migration-id mig))))
      (delete-rec (db migrations) rec))))

(defun migrate (&key id)
  (let ((result 0))
    (dolist (m *migrations*)
      (when (or (null id) (<= (migration-id m) id))
	 (unless (rec-exists? (db migrations) (migration-id m))
	   (migrate-up m)
	   (incf result))))

    (dolist (m (reverse *migrations*))
      (when (and id (> (migration-id m) id))
	(when (rec-exists? (db migrations) (migration-id m))
	  (migrate-down m)
	  (incf result))))

    result))
