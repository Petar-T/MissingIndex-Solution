# MissingIndex-Solution
Powershell library to collect, manage and implement missing indexes

Functions included

```powershell
Function MissingIndexes-Collect                 #v1.0
Function MissingIndexes-Check-CollectionDB      #v1.0
Function MissingIndexes-Index-Valid             #v1.0
Function MissingIndexes-Create                  #v1.0
```
**Steps : <br/>**

#1 : **Init collection database **
```powershell

MissingIndexes-Check-CollectionDB -DataWarehouseServer Petar_T -DataWarehouseDatabase 'SQL_Datawarehouse' -ServerList 'C:\Deploy\Query_Repository\SQLServerList.txt'
```

#2 : **collect missing indexes  **
```powershell

MissingIndexes-Collect -DataWarehouseServer Petar_T 
```

#3 : **validate and create missing indexes from collection  **
```powershell

MissingIndexes-Create -DataWarehouseServer Petar_T
```


