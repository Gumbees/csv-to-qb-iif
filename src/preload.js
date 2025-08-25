const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('api', {
  pickFile: () => ipcRenderer.invoke('pick-file'),
  dropCsv: (content) => ipcRenderer.invoke('drop-csv', content),
  exportIif: (bills, suggestedName) => ipcRenderer.invoke('export-iif', bills, suggestedName),
});


