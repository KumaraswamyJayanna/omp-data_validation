# omp-dataValidation
Validate and verify the Pipeline output and Groundtruths output

## Steps to configure and execute
- Create and Activate a virtual envirornment
- `python -m venv <Envirornmentname>`
- `source <Envirornmentname>\Scripts\activate`

## Install the project requirements in your virtual envirornment
- `pip install -r requirements.txt`

## To Generate or freeze the requirements
- `pip freeze > requirements.txt`

## Steps to generate reports
- checkout to new branch and use it(just to avoid conflicts)
-`git checkout -b <branchname>`
- copy the testdata i.e groundtruth and pipeline files in TestData, and update the testpath in config.py file
- output >> Pipeline output file
- GTPath >> Groundtruth File 

### To generate report of the data difference and highlights run below command
- `python.exe main.py`

### To generate report of category and file level summaries run below command
- `python.exe summary_report.py`

### Reports will be generated in the Reports Folder
- Directory ValidationData has the intermediate data after data preprocessing