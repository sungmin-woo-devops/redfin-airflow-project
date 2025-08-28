import json, subprocess

from airflow.exceptions import AirflowException

def tailscale_status(min_peers: int = 0) -> dict:

    # 1. tailscale 명령어 실행 (cf. cp for CompletedProcess)
    cp = subprocess.run(["tailscale", "status", "--json"], capture_output=True, text=True)

    # 2. 명령어 실행 결과 확인
    if cp.returncode != 0:
        raise AirflowException(f"[UTIL] tailscale status failed: {cp.stderr}")

    # 3. JSON 파싱
    data = json.loads(cp.stdout)
    peers = len(data.get("Peers", []))

    # 4. 최소 피어 수 확인
    if peers < min_peers:
        raise AirflowException(f"Tailscale peers {peers} < min_peers {min_peers}")

    # 5. 결과 반환
    return {"peers": peers, "backend": data.get("BackendState")}
