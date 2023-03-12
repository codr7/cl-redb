(in-package redb)

(defclass def ()
  ((name :initarg :name :initform (error "missing name") :reader name)))

(defmethod sql-name ((self def))
  (sql-name (name self)))

(defclass table-def (def)
  ((table :initarg :table :initform (error "missing table") :reader table)))

(defmethod initialize-instance :after ((def table-def) &key)
  (table-add (table def) def))
