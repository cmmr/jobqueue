## Resubmission
This is a resubmission. In this version I have:

* Removed extra spaces and single quotes from the DESCRIPTION description field.

* Not added method references to DOIs, ISBN, etc as there are none to reference.

* Renamed `wd` variable to `tmp` to indicate that it represents a tempdir() location.

* Ensured no files are written to non-tempdir() locations by default or in any 
  examples/vignettes/tests.
  


## R CMD check results

0 errors | 0 warnings | 1 note

* This is a new release.
