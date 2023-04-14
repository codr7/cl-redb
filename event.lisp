(in-package redb)

(defparameter *event-db*
  '((enum event-type
     user-created
     user-updated)

    (seq event-id)
    
    (table events (id)
     (col id bigint)
     (col type event-type)
     (col meta json :null? t)
     (col body json :null? t)
     (col at timestamp)
     (index at-idx nil at))))
