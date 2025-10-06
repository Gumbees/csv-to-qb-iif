async function generateBillsIif(db, bills) {
  const defaults = (await db.getDefaultQbdAccounts?.()) || {};
  const apAcc = defaults.accounts_payable || 'Accounts Payable';
  const invAssetAcc = defaults.inventory_asset || 'Inventory Asset';

  const lines = [];
  lines.push('!TRNS\tTRNSID\tTRNSTYPE\tDATE\tACCNT\tNAME\tCLASS\tAMOUNT\tDOCNUM\tMEMO\tCLEAR\tTOPRINT\tADDR5\tDUEDATE\tTERMS');
  lines.push('!SPL\tSPLID\tTRNSTYPE\tDATE\tACCNT\tNAME\tCLASS\tAMOUNT\tDOCNUM\tMEMO\tCLEAR\tQNTY\tPRICE\tINVITEM');
  lines.push('!ENDTRNS');
  for (const bill of bills || []) {
    lines.push(['TRNS','','BILL',bill.date,apAcc,bill.vendor,'',`-${Number(bill.total_amount || 0).toFixed(2)}`,bill.ref_num,'','N','N','',bill.due_date || '', bill.terms || ''].join('\t'));
    for (const line of bill.lines || []) {
      lines.push(['SPL','','BILL',bill.date,invAssetAcc,'','',Number(line.line_amount || 0).toFixed(2),'',line.description || '','N', String(line.quantity || ''), Number(line.unit_cost || 0).toFixed(2), line.item || ''].join('\t'));
    }
    lines.push('ENDTRNS');
  }
  return lines.join('\r\n') + '\r\n';
}

async function generateItemsIif(db) {
  const defaults = (await db.getDefaultQbdAccounts?.()) || {};
  const invAssetAcc = defaults.inventory_asset || 'Inventory Asset';
  const cogsAcc = defaults.cogs || 'Cost of Goods Sold';
  const incomeAcc = defaults.income || 'Sales';

  // Pull items from inventory summary; you can switch to catalog_items if preferred
  const inventory = await db.getInventoryForExport();
  let out = '!INVITEM\tNAME\tDESC\tPRICE\tCOST\tACCNT\tASSETACCNT\tCOGSACCNT\r\n';
  for (const row of inventory) {
    const name = row.item_name;
    const desc = row.description || '';
    const cost = Number(row.average_unit_cost || 0);
    const price = 0; // If you have catalog pricing, replace with that value
    out += `INVITEM\t"${name}"\t"${desc}"\t${price}\t${cost}\t"${incomeAcc}"\t"${invAssetAcc}"\t"${cogsAcc}"\r\n`;
  }
  return out;
}

module.exports = { generateBillsIif, generateItemsIif };
