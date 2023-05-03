(in-package redb)

(defun event-db ()
    `((enum event-type
       ,@(mapcar #'event-type-id *event-types*))
      
      (seq event-id)
      
      (table events (id)
       (col id bigint)
       (col type event-type)
       (col meta json :null? t)
       (col body json :null? t)
       (col at timestamp)
       (index at-idx nil at))))

(defvar *event-types* nil)

(defstruct event-type
  (id (error "Missing id") :type keyword)
  (exec (error "Missing exec") :type function))

(defun register-event-type (et)
  (push et *event-types*))

(defun get-event-type (id)
  (first (member id *event-types* :key #'event-type-id)))

(defun exec-event (evt)
  (funcall (event-type-exec (get-event-type (field evt (db events type)))) evt))

(defmacro define-event (id args &body body)
  `(register-event-type (make-event-type :id ,(kw id) :exec (lambda (,@args) ,@body))))
