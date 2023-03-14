(asdf:defsystem cl-redb
  :name "cl-redb"
  :version "1"
  :maintainer "codr7"
  :author "codr7"
  :description ""
  :licence "MIT"
  :depends-on ("cffi" "local-time")
  :serial t
  :components ((:file "redb")
	       (:file "util")
	       (:file "pg")
	       (:file "sql")
	       (:file "def")
	       (:file "cx")
	       (:file "tx")
	       (:file "col")
	       (:file "key")
	       (:file "foreign-key")
	       (:file "table")
	       (:file "enum")
	       (:file "db")
	       (:file "rec")
	       (:file "test")))
