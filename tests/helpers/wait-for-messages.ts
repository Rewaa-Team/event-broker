/**
 * Waits until the expected number of messages are received or timeout is reached.
 */
export function waitForMessages<T>(
  received: T[],
  expectedCount: number,
  timeoutMs = 15000
): Promise<T[]> {
  return new Promise((resolve, reject) => {
    const startTime = Date.now();

    const check = () => {
      if (received.length >= expectedCount) {
        resolve(received);
        return;
      }
      if (Date.now() - startTime > timeoutMs) {
        resolve(received); // resolve with whatever we got
        return;
      }
      setTimeout(check, 200);
    };

    check();
  });
}
