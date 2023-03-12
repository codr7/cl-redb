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
	       (:file "sql")
	       (:file "def")
	       (:file "cx")
	       (:file "col")
	       (:file "enum")
	       (:file "rec")
	       (:file "table")
	       (:file "db")
	       (:file "test")
	       (:file "redb")))
