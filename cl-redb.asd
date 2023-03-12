(asdf:defsystem cl-redb
  :name "cl-redb"
  :version "1"
  :maintainer "codr7"
  :author "codr7"
  :description ""
  :licence "MIT"
  :depends-on ("cffi" "cl-slog" "local-time")
  :serial t
  :components ((:file "util")
	       (:file "pg")
	       (:file "redb")
	       (:file "col")
	       (:file "enum")
	       (:file "table")
	       (:file "rec")
	       (:file "db")))
