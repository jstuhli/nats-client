<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\JetStream\Stream;

use Nats\JetStream\Stream\ExternalStream;
use Nats\JetStream\Stream\StreamSource;
use Nats\JetStream\Stream\SubjectTransform;
use PHPUnit\Framework\TestCase;

final class StreamSourceTest extends TestCase
{
    public function testToArrayWithSubjectTransforms(): void
    {
        $source = new StreamSource(
            name: 'origin',
            subjectTransforms: [
                new SubjectTransform(source: 'a.>', destination: 'b.>'),
                new SubjectTransform(source: 'c.>', destination: 'd.>'),
            ],
        );

        $array = $source->toArray();

        self::assertSame('origin', $array['name']);
        self::assertCount(2, $array['subject_transforms']);
        self::assertSame('a.>', $array['subject_transforms'][0]['src']);
        self::assertSame('b.>', $array['subject_transforms'][0]['dest']);
        self::assertSame('c.>', $array['subject_transforms'][1]['src']);
        self::assertSame('d.>', $array['subject_transforms'][1]['dest']);
    }

    public function testToArrayWithExternal(): void
    {
        $source = new StreamSource(
            name: 'remote',
            external: new ExternalStream(apiPrefix: '$JS.hub.API', deliverPrefix: '$JS.hub.ACK'),
        );

        $array = $source->toArray();

        self::assertSame('$JS.hub.API', $array['external']['api']);
        self::assertSame('$JS.hub.ACK', $array['external']['deliver']);
    }

    public function testToArrayWithDomainFallsBackToExternalApi(): void
    {
        $source = new StreamSource(name: 'remote', domain: 'hub');
        $array = $source->toArray();

        self::assertSame('$JS.hub.API', $array['external']['api']);
    }

    public function testToArrayExternalTakesPrecedenceOverDomain(): void
    {
        $source = new StreamSource(
            name: 'remote',
            domain: 'hub',
            external: new ExternalStream(apiPrefix: '$JS.custom.API'),
        );

        $array = $source->toArray();

        self::assertSame('$JS.custom.API', $array['external']['api']);
    }

    public function testToArrayOmitsEmptyTransforms(): void
    {
        $source = new StreamSource(name: 'simple');
        $array = $source->toArray();

        self::assertArrayNotHasKey('subject_transforms', $array);
        self::assertArrayNotHasKey('external', $array);
    }

    public function testFromArrayWithSubjectTransforms(): void
    {
        $data = [
            'name' => 'sourced',
            'subject_transforms' => [
                ['src' => 'foo.>', 'dest' => 'bar.>'],
            ],
        ];

        $source = StreamSource::fromArray($data);

        self::assertSame('sourced', $source->name);
        self::assertCount(1, $source->subjectTransforms);
        self::assertInstanceOf(SubjectTransform::class, $source->subjectTransforms[0]);
        self::assertSame('foo.>', $source->subjectTransforms[0]->source);
        self::assertSame('bar.>', $source->subjectTransforms[0]->destination);
    }

    public function testFromArrayWithExternal(): void
    {
        $data = [
            'name' => 'ext-stream',
            'external' => [
                'api' => '$JS.remote.API',
                'deliver' => '$JS.remote.ACK',
            ],
        ];

        $source = StreamSource::fromArray($data);

        self::assertInstanceOf(ExternalStream::class, $source->external);
        self::assertSame('$JS.remote.API', $source->external->apiPrefix);
        self::assertSame('$JS.remote.ACK', $source->external->deliverPrefix);
    }

    public function testFromArrayDomainExtractedFromExternal(): void
    {
        $data = [
            'name' => 'domain-source',
            'external' => ['api' => '$JS.myhub.API'],
        ];

        $source = StreamSource::fromArray($data);

        self::assertSame('$JS.myhub.API', $source->domain);
    }

    public function testFromArrayDefaults(): void
    {
        $source = StreamSource::fromArray(['name' => 'basic']);

        self::assertSame('basic', $source->name);
        self::assertNull($source->optStartSeq);
        self::assertNull($source->optStartTime);
        self::assertNull($source->filterSubject);
        self::assertNull($source->domain);
        self::assertEmpty($source->subjectTransforms);
        self::assertNull($source->external);
    }

    public function testRoundTripWithSubjectTransforms(): void
    {
        $original = new StreamSource(
            name: 'roundtrip',
            filterSubject: 'events.>',
            subjectTransforms: [
                new SubjectTransform(source: 'events.>', destination: 'copied.events.>'),
            ],
        );

        $restored = StreamSource::fromArray($original->toArray());

        self::assertSame($original->name, $restored->name);
        self::assertSame($original->filterSubject, $restored->filterSubject);
        self::assertCount(1, $restored->subjectTransforms);
        self::assertSame('events.>', $restored->subjectTransforms[0]->source);
        self::assertSame('copied.events.>', $restored->subjectTransforms[0]->destination);
    }
}
