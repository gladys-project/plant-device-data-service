package com.gladys.plantdevice.persistence.repository;

import com.gladys.plantdevice.persistence.model.DiagnosticHistoryDocument;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Flux;

import java.util.UUID;

public interface DiagnosticHistoryRepository extends ReactiveMongoRepository<DiagnosticHistoryDocument, String> {
    Flux<DiagnosticHistoryDocument> findByDeviceIdOrderByRecordedAtDescIdAsc(UUID deviceId, Pageable page);
}
