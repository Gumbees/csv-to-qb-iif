const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('api', {
  pickFile: () => ipcRenderer.invoke('pick-file'),
  dropCsv: (content, filename) => ipcRenderer.invoke('drop-csv', content, filename),
  exportIif: (bills, suggestedName) => ipcRenderer.invoke('export-iif', bills, suggestedName),
  processImport: (importId, bills) => ipcRenderer.invoke('process-import', importId, bills),
  
  // Database operations
  getDashboardStats: () => ipcRenderer.invoke('get-dashboard-stats'),
  getRecentTransactions: (hours) => ipcRenderer.invoke('get-recent-transactions', hours),
  getAllTransactions: () => ipcRenderer.invoke('get-all-transactions'),
  getInventorySummary: () => ipcRenderer.invoke('get-inventory-summary'),
  getInventoryForExport: () => ipcRenderer.invoke('get-inventory-for-export'),
  getImportHistory: () => ipcRenderer.invoke('get-import-history'),
  getExportHistory: () => ipcRenderer.invoke('get-export-history'),
  getItemTransactions: (itemId) => ipcRenderer.invoke('get-item-transactions', itemId),
});


