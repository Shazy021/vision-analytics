"""Классификация CUDA-ошибок для инференс-процессов (consumers/*).

Общая для yolo_frames и video консьюмеров: решение «убить процесс или
пропустить сообщение» принимается одинаково, пусть и живёт в одном месте.
"""


def is_fatal_cuda_error(exc: BaseException) -> bool:
    """Sticky CUDA error: torch.AcceleratorError или сообщение 'CUDA error'.

    Проверяем по имени класса, а не через isinstance(torch.AcceleratorError),
    чтобы не тянуть torch в импорт модуля (он нужен только в рантайме YOLO).
    OOM ('CUDA out of memory') сюда НЕ попадает — он не липкий.
    """
    if type(exc).__name__ == "AcceleratorError":
        return True
    text = str(exc)
    return "CUDA error" in text or "cuCtxSynchronize" in text
