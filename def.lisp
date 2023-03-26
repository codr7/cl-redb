(in-package redb)

(defclass def ()
  ((name :initarg :name :initform (error "missing name") :reader name)))

(defmethod sql-name ((def def))
  (sql-name (name def)))

(defmethod sql ((def def))
  (sql-name def))

(defmethod params ((def def)))

(defclass table-def (def)
  ((table :initarg :table :initform (error "missing table") :reader table)))

(defmethod initialize-instance :after ((def table-def) &key)
  (table-add (table def) def))
