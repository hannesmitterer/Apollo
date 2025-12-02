import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin';
import { ethers } from 'ethers';

// Initialize if not already done
if (!admin.apps.length) {
    admin.initializeApp();
}

const db = admin.firestore();

// Configuration with validation
const config = {
    rpcUrl: process.env.RPC_URL,
    ledgerAddress: process.env.ETHICAL_LEDGER_ADDRESS,
    thresholdDivergence: 0.005,
    maxBlockchainRetries: 3,
    alertCooldownMs: 3600000 // 1 hour
};

const ETHICAL_LEDGER_ABI_READ = [
    "function CORE_AXIOM_HASH() view returns (bytes32)"
];

/**
 * Calculate Keccak256 hash of current axiomatic weights
 */
async function calculateLocalAxiomHash(): Promise<string> {
    const configDoc = await db.collection('systemConfig')
        .doc('EVS_Active_Axioms')
        .get();
    
    if (!configDoc.exists) {
        throw new Error("SystemConfig EVS_Active_Axioms not found");
    }
    
    const { axiomaticWeights } = configDoc.data() || {};
    
    if (!axiomaticWeights || !Array.isArray(axiomaticWeights)) {
        throw new Error("Invalid or missing axiomatic weights");
    }

    // Deterministic serialization - critical for consistency
    const vectorString = JSON.stringify(
        axiomaticWeights.map(w => w.toString())
    );
    
    // Use actual Keccak256 (not SHA256)
    return ethers.keccak256(ethers.toUtf8Bytes(vectorString));
}

/**
 * Fetch immutable core axiom hash from blockchain with retry logic
 */
async function getBlockchainCoreHash(): Promise<string> {
    if (!config.rpcUrl || !config.ledgerAddress) {
        throw new functions.https.HttpsError(
            'failed-precondition',
            'Blockchain configuration missing'
        );
    }

    for (let attempt = 0; attempt < config.maxBlockchainRetries; attempt++) {
        try {
            const provider = new ethers.JsonRpcProvider(config.rpcUrl);
            const contract = new ethers.Contract(
                config.ledgerAddress,
                ETHICAL_LEDGER_ABI_READ,
                provider
            );
            
            const hash = await contract.CORE_AXIOM_HASH();
            return hash as string;
            
        } catch (error) {
            console.warn(`Blockchain fetch attempt ${attempt + 1} failed:`, error);
            
            if (attempt === config.maxBlockchainRetries - 1) {
                throw new Error(`Failed to fetch blockchain hash after ${config.maxBlockchainRetries} attempts`);
            }
            
            // Exponential backoff
            await new Promise(resolve => 
                setTimeout(resolve, 1000 * Math.pow(2, attempt))
            );
        }
    }
    
    throw new Error('Unexpected retry loop exit');
}

/**
 * Check if alert was recently triggered (circuit breaker)
 */
async function hasRecentAlert(): Promise<boolean> {
    const cutoff = new Date(Date.now() - config.alertCooldownMs);
    
    const recentAlerts = await db.collection('alerts')
        .where('type', '==', 'CORE_DRIFT')
        .where('timestamp', '>', cutoff)
        .limit(1)
        .get();
    
    return !recentAlerts.empty;
}

/**
 * Trigger multi-channel alert for council intervention
 */
async function triggerCouncilAlert(
    divergenceScore: number,
    localHash: string,
    anchoredHash: string
): Promise<void> {
    const alertDoc = {
        type: 'CORE_DRIFT',
        severity: 'CRITICAL',
        divergenceScore,
        localHash,
        anchoredHash,
        timestamp: admin.firestore.FieldValue.serverTimestamp(),
        acknowledged: false,
        message: 'Local axiom hash does not match immutable blockchain anchor'
    };
    
    // Store alert
    await db.collection('alerts').add(alertDoc);
    
    // Update governance protocol status
    await db.collection('governanceProtocols')
        .doc('MITHAQ_PROTOCOL_V2')
        .update({
            status: 'RED_ALARM_CORE_DRIFT',
            divergenceScore,
            lastDivergenceCheck: admin.firestore.FieldValue.serverTimestamp(),
            driftDetails: alertDoc.message
        });
    
    // TODO: Implement actual notification channels
    // - Email to council members
    // - SMS alerts
    // - Dashboard push notifications
    // - Discord/Slack webhooks
    
    console.error('🚨 COUNCIL ALERT TRIGGERED - Manual intervention required');
}

/**
 * Store audit result for historical analysis
 */
async function recordAuditResult(
    localHash: string,
    anchoredHash: string,
    divergenceScore: number,
    status: 'OK' | 'DRIFT'
): Promise<void> {
    await db.collection('auditHistory').add({
        timestamp: admin.firestore.FieldValue.serverTimestamp(),
        localHash,
        anchoredHash,
        divergenceScore,
        status,
        hashMatch: localHash.toLowerCase() === anchoredHash.toLowerCase()
    });
}

/**
 * Main Proof of Divergence audit function
 * Runs hourly to verify system integrity
 */
export const runProofOfDivergence = functions
    .runWith({ timeoutSeconds: 300, memory: '512MB' })
    .pubsub.schedule('0 * * * *')
    .timeZone('UTC')
    .onRun(async (context) => {
        console.log('--- KRP Shadow Watcher: Proof-of-Divergence Audit Started ---');
        
        try {
            // 1. Calculate current local axiom hash
            const localHash = await calculateLocalAxiomHash();
            console.log(`[PoD] Local Axiom Hash: ${localHash.substring(0, 18)}...`);
            
            // 2. Fetch immutable blockchain anchor hash
            const anchoredHash = await getBlockchainCoreHash();
            console.log(`[PoD] Anchor Hash: ${anchoredHash.substring(0, 18)}...`);
            
            // 3. Compare hashes (case-insensitive)
            const hashesMatch = localHash.toLowerCase() === anchoredHash.toLowerCase();
            const divergenceScore = hashesMatch ? 0.0 : 1.0;
            
            console.log(`[PoD] Divergence Score: ${divergenceScore.toFixed(4)}`);
            
            // 4. Record audit result
            await recordAuditResult(
                localHash,
                anchoredHash,
                divergenceScore,
                hashesMatch ? 'OK' : 'DRIFT'
            );
            
            // 5. Check threshold and trigger alerts if needed
            if (divergenceScore > config.thresholdDivergence) {
                console.error(`🚨 KRP RED ALARM: CORE DRIFT DETECTED (${divergenceScore.toFixed(4)})`);
                
                // Check circuit breaker
                if (await hasRecentAlert()) {
                    console.log('Alert cooldown active, skipping duplicate alert');
                } else {
                    await triggerCouncilAlert(divergenceScore, localHash, anchoredHash);
                }
                
            } else {
                console.log(`✅ KRP Status: INTEGRITY VERIFIED (${divergenceScore.toFixed(4)})`);
            }
            
            return null;
            
        } catch (error) {
            console.error('Fatal KRP error:', error);
            
            // Log critical system failure
            await db.collection('systemErrors').add({
                timestamp: admin.firestore.FieldValue.serverTimestamp(),
                type: 'KRP_AUDIT_FAILURE',
                error: error instanceof Error ? error.message : String(error),
                stack: error instanceof Error ? error.stack : undefined
            });
            
            throw error;
        }
    });
