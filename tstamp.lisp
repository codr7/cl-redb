(in-package redb)

(defstruct tstamp
  (year (error "Missing year") :type integer)
  (month (error "Missing month") :type integer)
  (day (error "Missing day") :type integer)
  (hour 0 :type integer)
  (minute 0 :type integer)
  (second 0 :type integer))

(defun tstamp-to-sql (val)
  (format nil "~a-~a-~a ~a:~a:~a"
	  (tstamp-year val) (tstamp-month val) (tstamp-day val)
	  (tstamp-hour val) (tstamp-minute val) (tstamp-second val)))

(defun tstamp-from-sql (val)
  (flet ((p (i)
	   (parse-integer val :start i :junk-allowed t)))
    (let ((year (p 0))
	  (month (p 5))
	  (day (p 8))
	  (hour (p 11))
	  (minute (p 14))
	  (second (p 17)))
      (make-tstamp :year year :month month :day day
		 :hour hour :minute minute :second second))))

(defun now ()
  (tstamp-from-sql (send-val "SELECT LOCALTIMESTAMP" nil)))

(defun tstamp= (x y)
  (and (= (tstamp-year x) (tstamp-year y))
       (= (tstamp-month x) (tstamp-month y))
       (= (tstamp-day x) (tstamp-day y))
       (= (tstamp-hour x) (tstamp-hour y))
       (= (tstamp-minute x) (tstamp-minute y))
       (= (tstamp-second x) (tstamp-second y))))

