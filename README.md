1. District court: https://services.ecourts.gov.in/ecourtindia_v6/
2. High Court:
   a. Gujarat High Court: https://gujarathighcourt.nic.in
   b. Bombay High Court: https://bombayhighcourt.nic.in/index.php
   c. Delhi High Court: https://delhihighcourt.nic.in/web/
   d. Remaining: https://hcservices.ecourts.gov.in/hcservices/main.php
3. SCI: https://www.sci.gov.in/case-status-case-no/
4. NCLT: https://nclt.gov.in/case-number-wise
5. NCLAT: https://nclat.nic.in/display-board/cases
6. ITAT: https://itat.gov.in/judicial/casestatus
7. CESTAT: https://cestat.gov.in/casestatus
8. DRT: https://drt.gov.in/#/casedetail

# Uses

1. fetch basic case data
2. fetch case details + save orders to db on request
3. update next hearing date using cron job, cases are updated on the day of the hearing around evening + send alert
4. fetch daily cause_list + send alert to the users who have their case listed.

# Case numbers:

- Use any arbitary number as case number for searching
- Use any year
- If neither works, use any surcommon name like `Singh`, `Gupta`, `Tiwari` to find cases and use their case number.
  [TO BE UPDATED]
