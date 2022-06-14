#pragma once
#ifndef LLFS_BASIC_RING_BUFFER_DEVICE_HPP
#define LLFS_BASIC_RING_BUFFER_DEVICE_HPP

#include <llfs/basic_log_storage_driver.hpp>
#include <llfs/basic_log_storage_reader.hpp>
#include <llfs/log_device.hpp>
#include <llfs/ring_buffer.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//

class LogDeviceSnapshot;

template <class Impl>
class BasicRingBufferDevice
    : public LogDevice
    , protected LogStorageDriverContext
{
 public:
  friend class LogDeviceSnapshot;

  using driver_type = BasicLogStorageDriver<Impl>;

  class WriterImpl;

  explicit BasicRingBufferDevice(const RingBuffer::Params& params) noexcept;

  template <typename... Args, typename = batt::EnableIfNoShadow<
                                  BasicRingBufferDevice, const RingBuffer::Params&, Args&&...>>
  explicit BasicRingBufferDevice(const RingBuffer::Params& params, Args&&... args) noexcept;

  ~BasicRingBufferDevice() noexcept;

  u64 capacity() const override;

  u64 size() const override;

  Status trim(slot_offset_type slot_lower_bound) override;

  std::unique_ptr<Reader> new_reader(Optional<slot_offset_type> slot_lower_bound,
                                     LogReadMode mode) override;

  SlotRange slot_range(LogReadMode mode) override;

  Writer& writer() override;

  Status open()
  {
    return this->driver_.open();
  }

  Status close() override;

  Status sync(LogReadMode mode, SlotUpperBoundAt event) override;

  driver_type& driver()
  {
    return this->driver_;
  }

 private:
  std::unique_ptr<WriterImpl> writer_ = std::make_unique<WriterImpl>(this);
  driver_type driver_{*this};
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class BasicRingBufferDevice
//

template <class Impl>
inline BasicRingBufferDevice<Impl>::BasicRingBufferDevice(const RingBuffer::Params& params) noexcept
    : LogStorageDriverContext{params}
{
}

template <class Impl>
template <typename... Args, typename>
inline BasicRingBufferDevice<Impl>::BasicRingBufferDevice(const RingBuffer::Params& params,
                                                          Args&&... args) noexcept
    : LogStorageDriverContext{params}
    , driver_{*this, BATT_FORWARD(args)...}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <class Impl>
inline BasicRingBufferDevice<Impl>::~BasicRingBufferDevice() noexcept
{
  this->close().IgnoreError();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <class Impl>
inline u64 BasicRingBufferDevice<Impl>::capacity() const
{
  return this->buffer_.size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <class Impl>
inline u64 BasicRingBufferDevice<Impl>::size() const
{
  return slot_distance(this->driver_.get_trim_pos(), this->driver_.get_commit_pos());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <class Impl>
inline Status BasicRingBufferDevice<Impl>::trim(slot_offset_type slot_lower_bound)
{
  BATT_CHECK_LE(this->driver_.get_trim_pos(), slot_lower_bound);

  return this->driver_.set_trim_pos(slot_lower_bound);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <class Impl>
inline std::unique_ptr<LogDevice::Reader> BasicRingBufferDevice<Impl>::new_reader(
    Optional<slot_offset_type> slot_lower_bound, LogReadMode mode)
{
  auto& context = static_cast<LogStorageDriverContext&>(*this);
  return std::make_unique<BasicLogStorageReader<Impl>>(
      context, /*driver=*/this->driver_, mode,
      slot_lower_bound.value_or(this->driver_.get_trim_pos()));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <class Impl>
inline SlotRange BasicRingBufferDevice<Impl>::slot_range(LogReadMode mode)
{
  if (mode == LogReadMode ::kDurable) {
    return SlotRange{this->driver_.get_trim_pos(), this->driver_.get_flush_pos()};
  }
  return SlotRange{this->driver_.get_trim_pos(), this->driver_.get_commit_pos()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <class Impl>
inline LogDevice::Writer& BasicRingBufferDevice<Impl>::writer()
{
  return *this->writer_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <class Impl>
inline Status BasicRingBufferDevice<Impl>::close()
{
  const bool closed_prior = this->closed_.exchange(true);
  if (!closed_prior) {
    return this->driver_.close();
  }
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <class Impl>
inline Status BasicRingBufferDevice<Impl>::sync(LogReadMode mode, SlotUpperBoundAt event)
{
  switch (mode) {
    case LogReadMode::kInconsistent:
      return OkStatus();

    case LogReadMode::kSpeculative:
      return this->driver_.await_commit_pos(event.offset).status();

    case LogReadMode::kDurable:
      return this->driver_.await_flush_pos(event.offset).status();
  }

  BATT_PANIC() << "bad LogReadMode value: " << (unsigned)mode;
  BATT_UNREACHABLE();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class BasicRingBufferDevice::WriterImpl
//

template <class Impl>
class BasicRingBufferDevice<Impl>::WriterImpl : public LogDevice::Writer
{
 public:
  explicit WriterImpl(BasicRingBufferDevice<Impl>* device) noexcept : device_{device}
  {
    initialize_status_codes();
  }

  slot_offset_type slot_offset() override
  {
    return this->device_->driver_.get_commit_pos();
  }

  std::size_t space() const
  {
    const slot_offset_type readable_begin = this->device_->driver_.get_trim_pos();
    const slot_offset_type readable_end = this->device_->driver_.get_commit_pos();

    const std::size_t space_available =
        this->device_->buffer_.size() - slot_distance(readable_begin, readable_end);

    return space_available;
  }

  StatusOr<MutableBuffer> prepare(std::size_t byte_count, std::size_t head_room) override
  {
    BATT_CHECK(!this->prepared_offset_);

    if (this->device_->closed_.load()) {
      return Status{StatusCode::kPrepareFailedLogClosed};
    }

    const std::size_t space_required = byte_count + head_room;

    if (this->space() < space_required) {
      return Status{StatusCode::kPrepareFailedTrimRequired};
    }

    const slot_offset_type commit_pos = this->device_->driver_.get_commit_pos();
    MutableBuffer writable_region = this->device_->buffer_.get_mut(commit_pos);

    this->prepared_offset_ = commit_pos;

    return MutableBuffer{writable_region.data(), byte_count};
  }

  StatusOr<slot_offset_type> commit(std::size_t byte_count) override
  {
    auto guard = batt::finally([&] {
      this->prepared_offset_ = None;
    });

    if (this->device_->closed_.load()) {
      return Status{StatusCode::kCommitFailedLogClosed};
    }

    BATT_CHECK(this->prepared_offset_);

    const slot_offset_type new_offset = *this->prepared_offset_ + byte_count;

    Status status = this->device_->driver_.set_commit_pos(new_offset);

    BATT_REQUIRE_OK(status);

    return new_offset;
  }

  Status await(WriterEvent event) override
  {
    return batt::case_of(
        event,
        [&](const SlotLowerBoundAt& trim_lower_bound_at) -> Status {
          BATT_DEBUG_INFO("Writer::await(SlotLowerBoundAt{"
                          << trim_lower_bound_at.offset << "})"
                          << " buffer_size=" << this->device_->buffer_.size() << " space="
                          << this->space() << " trim_pos=" << this->device_->driver_.get_trim_pos()
                          << " flush_pos=" << this->device_->driver_.get_flush_pos()
                          << " commit_pos=" << this->device_->driver_.get_commit_pos());

          return this->device_->driver_.await_trim_pos(trim_lower_bound_at.offset).status();
        },
        [&](const BytesAvailable& prepare_available) -> Status {
          BATT_DEBUG_INFO("Writer::await(BytesAvailable{" << prepare_available.size << "})");

          const slot_offset_type data_upper_bound = this->device_->driver_.get_commit_pos();

          return this->await(SlotLowerBoundAt{
              .offset = data_upper_bound - slot_offset_type{this->device_->buffer_.size()} +
                        prepare_available.size,
          });
        });
  }

 private:
  BasicRingBufferDevice<Impl>* device_;
  Optional<slot_offset_type> prepared_offset_;
};

}  // namespace llfs

#endif  // LLFS_BASIC_RING_BUFFER_DEVICE_HPP
