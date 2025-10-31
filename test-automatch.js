const Database = require('./src/database');
const StripeAPI = require('./src/stripe-api');

// Copy the improved matching functions from server.js
function calculateMatchScore(stripeCustomer, haloClient) {
    let score = 0;
    
    // Email match (most reliable) - requires both emails to be present
    if (stripeCustomer.email && haloClient.email) {
        const email1 = stripeCustomer.email.toLowerCase().trim();
        const email2 = haloClient.email.toLowerCase().trim();
        if (email1 === email2) {
            score += 0.8;
        }
    }
    
    // Improved name similarity matching with better scoring
    if (stripeCustomer.name && haloClient.name) {
        const name1 = stripeCustomer.name.toLowerCase().trim();
        const name2 = haloClient.name.toLowerCase().trim();
        
        // Human name matching - check if both appear to be personal names
        const isHumanName1 = isLikelyHumanName(name1);
        const isHumanName2 = isLikelyHumanName(name2);
        
        if (isHumanName1 && isHumanName2) {
            // Special handling for human names
            const humanScore = calculateHumanNameMatchScore(name1, name2);
            score += humanScore;
        } else {
            // Business/organization name matching
            // Exact match
            if (name1 === name2) {
                score += 0.8;
            }
            // One name is a clear subset of the other (e.g., "3 Bridges Pediatric" vs "3 Bridges Pediatric Dentistry")
            else if (isClearSubset(name1, name2)) {
                score += 0.7;
            }
            // Contains match with high confidence
            else if (name1.includes(name2) || name2.includes(name1)) {
                score += 0.6;
            }
            // Strong word overlap (most words match)
            else if (calculateWordOverlap(name1, name2) > 0.8) {
                score += 0.6;
            }
            // Partial match using common business name variations
            else if (hasStrongCommonWords(name1, name2)) {
                score += 0.5;
            }
            // Moderate word overlap
            else if (calculateWordOverlap(name1, name2) > 0.6) {
                score += 0.4;
            }
            // Weak word overlap but still meaningful
            else if (calculateWordOverlap(name1, name2) > 0.4) {
                score += 0.3;
            }
        }
    }
    
    return Math.min(score, 1.0);
}

// Check if a name appears to be a human name (personal name)
function isLikelyHumanName(name) {
    // Names with 2-4 words that don't contain obvious business terms
    const businessTerms = ['dental', 'dentistry', 'clinic', 'center', 'group', 'associates', 
                          'pediatric', 'orthodontics', 'care', 'practice', 'family', 'surgery', 
                          'smile', 'hospital', 'medical', 'doctor', 'dr', 'md', 'dds', 'llc',
                          'inc', 'corp', 'company', 'enterprises', 'department', 'center'];
    
    const words = name.split(/\s+/);
    
    // Too many words for a personal name
    if (words.length > 4) return false;
    
    // Contains business-related terms
    if (businessTerms.some(term => name.includes(term))) return false;
    
    // Most personal names have 2-3 words (First Last or First Middle Last)
    return words.length >= 2 && words.length <= 4;
}

// Calculate score for human name matching
function calculateHumanNameMatchScore(name1, name2) {
    let score = 0;
    
    const names1 = extractNameParts(name1);
    const names2 = extractNameParts(name2);
    
    // Exact match
    if (name1 === name2) {
        score += 0.8;
    }
    // Same name components in different order (e.g., "John Smith" vs "Smith, John")
    else if (hasSameNameComponents(names1, names2)) {
        score += 0.7;
    }
    // Last name match with first name similarity
    else if (names1.lastName && names2.lastName && names1.lastName === names2.lastName) {
        score += 0.6;
        if (names1.firstName && names2.firstName && 
            (names1.firstName.includes(names2.firstName) || names2.firstName.includes(names1.firstName))) {
            score += 0.2;
        }
    }
    // First name match
    else if (names1.firstName && names2.firstName && names1.firstName === names2.firstName) {
        score += 0.5;
    }
    // Partial name component matching
    else if (hasPartialNameMatch(names1, names2)) {
        score += 0.4;
    }
    
    return Math.min(score, 1.0);
}

// Extract name parts from a string
function extractNameParts(name) {
    const parts = {
        firstName: null,
        lastName: null,
        middleName: null,
        suffix: null
    };
    
    // Remove common honorifics and suffixes
    const cleanedName = name.replace(/\b(dr\.?|doctor|mr\.?|mrs\.?|ms\.?|prof\.?|professor)\b/gi, '').trim();
    const words = cleanedName.split(/\s+|,/).filter(word => word.length > 0);
    
    // Handle "Last, First" format
    if (name.includes(',')) {
        if (words.length >= 2) {
            parts.lastName = words[0];
            parts.firstName = words[1];
            // Check for middle name or suffix in remaining words
            if (words.length >= 3) {
                const remaining = words.slice(2).join(' ');
                if (isLikelySuffix(remaining)) {
                    parts.suffix = remaining;
                } else {
                    parts.middleName = remaining;
                }
            }
        }
    } else {
        // Handle "First Last" format
        if (words.length >= 1) parts.firstName = words[0];
        if (words.length >= 2) {
            // Check if last word is a suffix
            const lastWord = words[words.length - 1];
            if (isLikelySuffix(lastWord)) {
                parts.suffix = lastWord;
                parts.lastName = words.length >= 3 ? words[words.length - 2] : null;
            } else {
                parts.lastName = lastWord;
            }
        }
        if (words.length >= 3) {
            // Middle name is everything between first and last
            parts.middleName = words.slice(1, words.length - (parts.suffix ? 2 : 1)).join(' ');
        }
    }
    
    return parts;
}

// Check if a word is likely a name suffix
function isLikelySuffix(word) {
    const suffixes = ['jr', 'sr', 'ii', 'iii', 'iv', 'esq', 'phd', 'md', 'dds'];
    return suffixes.includes(word.toLowerCase().replace('.', ''));
}

// Check if two names have the same components regardless of order
function hasSameNameComponents(names1, names2) {
    const components1 = [names1.firstName, names1.lastName].filter(Boolean);
    const components2 = [names2.firstName, names2.lastName].filter(Boolean);
    
    if (components1.length === 0 || components2.length === 0) return false;
    
    // Check if both names contain the same set of name components
    const set1 = new Set(components1);
    const set2 = new Set(components2);
    
    if (set1.size !== set2.size) return false;
    
    for (const component of set1) {
        if (!set2.has(component)) return false;
    }
    
    return true;
}

// Check for partial name matching (one name contains components of the other)
function hasPartialNameMatch(names1, names2) {
    const allComponents1 = [names1.firstName, names1.lastName, names1.middleName].filter(Boolean);
    const allComponents2 = [names2.firstName, names2.lastName, names2.middleName].filter(Boolean);
    
    // Check if any component from name1 appears in name2 or vice versa
    for (const comp1 of allComponents1) {
        for (const comp2 of allComponents2) {
            if (comp1.includes(comp2) || comp2.includes(comp1)) {
                return true;
            }
        }
    }
    
    return false;
}

// Check if one name is a clear subset of the other (e.g., short form vs full form)
function isClearSubset(name1, name2) {
    const shorter = name1.length < name2.length ? name1 : name2;
    const longer = name1.length < name2.length ? name2 : name1;
    
    // The shorter name should be at least 70% of the longer name and be contained within it
    const lengthRatio = shorter.length / longer.length;
    return lengthRatio > 0.7 && longer.includes(shorter);
}

// Calculate word overlap between two names
function calculateWordOverlap(name1, name2) {
    const words1 = new Set(name1.split(/[\s,.&]+/).filter(word => word.length > 2));
    const words2 = new Set(name2.split(/[\s,.&]+/).filter(word => word.length > 2));
    
    if (words1.size === 0 || words2.size === 0) return 0;
    
    const intersection = new Set([...words1].filter(x => words2.has(x)));
    const union = new Set([...words1, ...words2]);
    
    return intersection.size / union.size;
}

// More sophisticated common word matching
function hasStrongCommonWords(name1, name2) {
    const businessWords = ['dental', 'dentistry', 'clinic', 'center', 'group', 'associates', 'pediatric', 'orthodontics', 'care', 'practice', 'family', 'surgery', 'smile'];
    
    const words1 = name1.split(/[\s,.&]+/).filter(word => word.length > 2);
    const words2 = name2.split(/[\s,.&]+/).filter(word => word.length > 2);
    
    // Count matching unique words (excluding very common business words)
    const uniqueWords1 = words1.filter(word => !businessWords.includes(word));
    const uniqueWords2 = words2.filter(word => !businessWords.includes(word));
    
    // Check for strong matching (at least 2 unique words match)
    let matchCount = 0;
    for (const word1 of uniqueWords1) {
        for (const word2 of uniqueWords2) {
            if (word1.includes(word2) || word2.includes(word1)) {
                matchCount++;
                if (matchCount >= 2) return true;
            }
        }
    }
    
    return false;
}

async function testAutomatch() {
    try {
        const db = new Database();
        const stripeAPI = new StripeAPI(db);
        
        console.log('=== Testing Improved Automatching Algorithm ===\n');
        
        // Get threshold from configuration
        const thresholdRow = await db.get('SELECT value FROM config WHERE key = ?', ['customer_match_threshold']);
        const threshold = thresholdRow ? parseFloat(thresholdRow.value) : 0.65;
        
        // Get customers from local database (already synced)
        const stripeCustomers = await stripeAPI.getImportedCustomers();
        const haloClients = await db.all(`
            SELECT id, halopsa_id, name, email, phone, address, 
                   created_at, last_sync, raw_data
            FROM halopsa_clients 
        `);
        
        // Filter out stripe customers with null names
        const validStripeCustomers = stripeCustomers.filter(customer => customer.name && customer.name !== 'null');
        
        console.log(`Total Stripe customers: ${stripeCustomers.length} (${validStripeCustomers.length} with valid names)`);
        console.log(`Total HaloPSA clients: ${haloClients.length}\n`);
        
        const matches = [];
        const detailedAnalysis = [];
        
        console.log(`Testing with threshold: ${threshold}\n`);
        
        // Sample first 20 valid stripe customers for testing
        const sampleStripeCustomers = validStripeCustomers.slice(0, 20);
        
        for (const stripeCustomer of sampleStripeCustomers) {
            console.log(`Analyzing Stripe customer: "${stripeCustomer.name}"`);
            
            let bestMatch = null;
            let bestScore = 0;
            const topMatches = [];
            
            for (const haloClient of haloClients) {
                const score = calculateMatchScore(stripeCustomer, haloClient);
                
                if (score > 0.1) { // Show any meaningful match
                    topMatches.push({ client: haloClient, score });
                }
                
                if (score > bestScore) {
                    bestScore = score;
                    bestMatch = haloClient;
                }
            }
            
            // Show top 3 matches for this customer
            topMatches.sort((a, b) => b.score - a.score);
            const top3 = topMatches.slice(0, 3);
            
            if (top3.length > 0) {
                console.log(`  Top matches:`);
                top3.forEach(match => {
                    console.log(`    - "${match.client.name}" (score: ${match.score.toFixed(2)})`);
                });
            } else {
                console.log(`  No matches found (max score: ${bestScore.toFixed(2)})`);
            }
            
            if (bestMatch && bestScore > threshold) {
                matches.push({
                    stripe_customer: stripeCustomer,
                    halo_client: bestMatch,
                    match_score: bestScore
                });
                console.log(`  ✅ MATCH FOUND: "${bestMatch.name}" (score: ${bestScore.toFixed(2)})\n`);
            } else {
                console.log(`  ❌ No match above threshold\n`);
            }
            
            detailedAnalysis.push({
                stripe_customer: stripeCustomer,
                best_match: bestMatch,
                best_score: bestScore,
                total_potential_matches: topMatches.length
            });
        }
        
        console.log(`=== SUMMARY ===`);
        console.log(`Total matches found above threshold: ${matches.length}`);
        console.log(`Percentage with matches: ${((matches.length / sampleStripeCustomers.length) * 100).toFixed(1)}%`);
        
        // Show why some customers didn't match
        console.log(`\n=== REASONS FOR NO MATCHES ===`);
        const noMatches = detailedAnalysis.filter(a => a.best_score < threshold);
        
        noMatches.slice(0, 5).forEach(analysis => {
            console.log(`Stripe: "${analysis.stripe_customer.name}"`);
            if (analysis.best_match) {
                console.log(`  Best potential: "${analysis.best_match.name}" (score: ${analysis.best_score.toFixed(2)})`);
            } else {
                console.log(`  No potential matches found`);
            }
            console.log(`  Reason: Score below threshold (${analysis.best_score.toFixed(2)} < ${threshold})`);
            console.log('');
        });
        
        db.close();
        
    } catch (error) {
        console.error('Error testing automatch:', error);
    }
}

testAutomatch();